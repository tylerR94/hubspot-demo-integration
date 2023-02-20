const hubspot = require("@hubspot/api-client");
const moment = require("moment");
const { queue } = require("async");
const _ = require("lodash");

const {
  filterNullValuesFromObject,
  normalizePropertyName,
  goal,
} = require("./utils");
const Domain = require("./Domain");

const hubspotClient = new hubspot.Client({ accessToken: "" });
const propertyPrefix = "hubspot__";
let expirationDate;

const generateLastModifiedDateFilter = (
  date,
  nowDate,
  propertyName = "hs_lastmodifieddate"
) => {
  const lastModifiedDateFilter = date
    ? {
        filters: [
          // { propertyName, operator: "BETWEEN", value: `${date.valueOf()}`, highValue: `${nowDate.valueOf()}` },
          { propertyName, operator: "GTE", value: `${date.valueOf()}` },
          { propertyName, operator: "LTE", value: `${nowDate.valueOf()}` },
        ],
      }
    : {};

  return lastModifiedDateFilter;
};

const saveDomain = async (domain) => {
  // disable this for testing purposes
  return;

  domain.markModified("integrations.hubspot.accounts");
  await domain.save();
};

/**
 * Get access token from HubSpot
 */
const refreshAccessToken = async (domain, hubId, tryCount) => {
  const { HUBSPOT_CID, HUBSPOT_CS } = process.env;
  const account = domain.integrations.hubspot.accounts.find(
    (account) => account.hubId === hubId
  );
  const { accessToken, refreshToken } = account;

  return hubspotClient.oauth.tokensApi
    .createToken(
      "refresh_token",
      undefined,
      undefined,
      HUBSPOT_CID,
      HUBSPOT_CS,
      refreshToken
    )
    .then(async (result) => {
      const body = result.body ? result.body : result;

      const newAccessToken = body.accessToken;
      expirationDate = new Date(body.expiresIn * 1000 + new Date().getTime());

      hubspotClient.setAccessToken(newAccessToken);
      if (newAccessToken !== accessToken) {
        account.accessToken = newAccessToken;
        await domain.save();
      }

      return true;
    });
};

/**
 * Get search results for a given engagement type, filtered by lastPulledDate so you only see new records.
 *
 * @param engagement {string} - the engagement type to process: 'contacts', 'companies', 'deals', 'meetings', etc.
 * @param searchFunction {function} - the function to call to get the search results. This function is different for each engagement type due to HubSpot's API design.
 * @param propertiesToCapture {string[]} - the properties to capture from the engagement
 * @param callbackBetweenPages {function} - a callback function to run in between pages of results. This exist so that you can act on data and then get rid of it before moving onto the next page, to avoid vertical scaling issues.
 * @param domain {object} - the domain object
 * @param hubId {string} - the hubId of the account to process
 * @param q {object} - the queue object which we are inheriting from the initial call and passing down to the callback function
 * @returns {Promise<void>}
 */
async function searchEngagement(
  engagement,
  searchFunction,
  propertiesToCapture,
  callbackBetweenPages,
  domain,
  hubId,
  q
) {
  const account = domain.integrations.hubspot.accounts.find(
    (account) => account.hubId === hubId
  );

  const lastPulledDate = new Date(
    account.lastPulledDates[engagement] || account.lastPulledDate
  );
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = 100;

  console.log(`searching for ${engagement}...`);
  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(
      lastModifiedDate,
      now,
      engagement === "contacts" ? "lastmodifieddate" : "hs_lastmodifieddate"
    );

    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: "lastmodifieddate", direction: "ASCENDING" }],
      properties: propertiesToCapture,
      limit,
      after: offsetObject.after,
    };

    let searchResult = {};

    let tryCount = 0;

    while (tryCount <= 4) {
      try {
        searchResult = await searchFunction(searchObject);
        break;
      } catch (err) {
        console.error(err);
        tryCount++;

        if (new Date() > expirationDate)
          await refreshAccessToken(domain, hubId);

        await new Promise((resolve, reject) =>
          setTimeout(resolve, 5000 * Math.pow(2, tryCount))
        );
      }
    }

    if (!searchResult)
      throw new Error(
        `Failed to fetch ${engagement} for the 4th time. Aborting.`
      );

    const data = searchResult.results || [];

    // act on this pages data before moving onto the next page
    await callbackBetweenPages(data, q, lastPulledDate);

    offsetObject.after = parseInt(searchResult.paging?.next?.after);

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(
        data[data.length - 1].updatedAt
      ).valueOf();
    }
  }

  console.log(`finished searching for ${engagement}....`);

  account.lastPulledDates[engagement] = now;
  await saveDomain(domain);

  return true;
}

/**
 * The reference function for processing meetings through the HubSpot API. This is necessary as passing HubSpot's function as an argument to searchEngagement() doesn't work and generates an error. There may be a workaround for this, but for the sake of this test I didn't spend much time resolving that.
 *
 * @param searchObject {object} - the search object to pass to HubSpot's API
 * @returns {Promise<Void>}
 */
async function hs_processCompanies(searchObject) {
  return hubspotClient.crm.companies.searchApi.doSearch(searchObject);
}

/**
 * Process recent companies through the HubSpot API. This function leverages a modular searchEngagement() function to handle the request as well as pagination and lastPulledDate logic.
 *
 * @param domain {object} - the domain object
 * @param hubId {string} - the hubId of the account to process
 * @param q {object} - the queue object which we are inheriting from the initial call and passing down to the callback function
 * @returns {Promise<Void>}
 */
async function processCompanies(domain, hubId, q) {
  await searchEngagement(
    "companies",
    hs_processCompanies,
    [
      "name",
      "domain",
      "country",
      "industry",
      "description",
      "annualrevenue",
      "numberofemployees",
      "hs_lead_status",
    ],
    processCompaniesCallback,
    domain,
    hubId,
    q
  );
}

/**
 * The callback function for processCompanies(). This function is called in between pages of results and is where you should act on companies data to create and push actions.
 *
 * @param data {object[]} - the data to process
 * @param q {object} - the queue object which we are inheriting from the initial call and using to push actions to a queue.
 * @param lastPulledDate {Date} - the last time we pulled data from HubSpot
 * @returns {Promise<Void>}
 */
async function processCompaniesCallback(data, q, lastPulledDate) {
  await data.forEach((company) => {
    if (!company.properties) return;

    const actionTemplate = {
      includeInAnalytics: 0,
      userId: company.id,
      identity: company.id,
      userProperties: { company_id: company.id },
      shared: [
        {
          key: "company_id",
          value: company.id,
          properties: {
            company_id: company.id,
            company_name: company.properties.name?.trim(),
            company_domain: company.properties.domain?.replace(
              /https:\/\/|http:\/\/|www./g,
              ""
            ),
            company_industry: company.properties.industry,
            company_number_of_employees: parseInt(
              company.properties.numberofemployees
            ),
            company_annual_revenue: parseInt(company.properties.annualrevenue),
            company_country: company.properties.country,
            company_description: company.properties.description,
          },
        },
      ],
    };

    const isCreated =
      !lastPulledDate || new Date(company.createdAt) > lastPulledDate;

    q.push({
      actionName: isCreated ? "Company Created" : "Company Updated",
      actionDate:
        new Date(isCreated ? company.createdAt : company.updatedAt) - 2000,
      ...actionTemplate,
    });
  });
}

/**
 * The reference function for processing contacts through the HubSpot API. This is necessary as passing HubSpot's function as an argument to searchEngagement() doesn't work and generates an error. There may be a workaround for this, but for the sake of this test I didn't spend much time resolving that.
 *
 * @param searchObject {object} - the search object to pass to HubSpot's API
 * @returns {Promise<Void>}
 */
async function hs_processContacts(searchObject) {
  return hubspotClient.crm.contacts.searchApi.doSearch(searchObject);
}

/**
 * Process recent contacts through the HubSpot API. This function leverages a modular searchEngagement() function to handle the request as well as pagination and lastPulledDate logic.
 *
 * @param domain {object} - the domain object
 * @param hubId {string} - the hubId of the account to process
 * @param q {object} - the queue object which we are inheriting from the initial call and passing down to the callback function
 * @returns {Promise<Void>}
 */
async function processContacts(domain, hubId, q) {
  await searchEngagement(
    "contacts",
    hs_processContacts,
    [
      "firstname",
      "lastname",
      "jobtitle",
      "email",
      "hubspotscore",
      "hs_lead_status",
      "hs_analytics_source",
      "hs_latest_source",
    ],
    processContactsCallback,
    domain,
    hubId,
    q
  );
}

/**
 * The callback function for processContacts(). This function is called in between pages of results and is where you should act on contacts data to create and push actions.
 *
 * @param data {object[]} - the data to process
 * @param q {object} - the queue object which we are inheriting from the initial call and using to push actions to a queue.
 * @param lastPulledDate {Date} - the last time we pulled data from HubSpot
 * @returns {Promise<Void>}
 */
async function processContactsCallback(data, q, lastPulledDate) {
  const contactIds = data.map((contact) => contact.id);

  // contact to company association
  const contactsToAssociate = contactIds;
  const companyAssociationsResults =
    (
      await (
        await hubspotClient.apiRequest({
          method: "post",
          path: "/crm/v3/associations/CONTACTS/COMPANIES/batch/read",
          body: {
            inputs: contactsToAssociate.map((contactId) => ({
              id: contactId,
            })),
          },
        })
      ).json()
    )?.results || [];

  // console.log(JSON.stringify(companyAssociationsResults));
  const companyAssociations = Object.fromEntries(
    companyAssociationsResults
      .map((a) => {
        if (a.from) {
          contactsToAssociate.splice(contactsToAssociate.indexOf(a.from.id), 1);
          return [a.from.id, a.to[0].id];
        } else return false;
      })
      .filter((x) => x)
  );

  data.forEach((contact) => {
    if (!contact.properties || !contact.properties.email) return;

    const companyId = companyAssociations[contact.id];

    const isCreated = new Date(contact.createdAt) > lastPulledDate;

    const userProperties = {
      company_id: companyId,
      contact_name: (
        (contact.properties.firstname || "") +
        " " +
        (contact.properties.lastname || "")
      ).trim(),
      contact_title: contact.properties.jobtitle,
      contact_source: contact.properties.hs_analytics_source,
      [propertyPrefix + "contact_id"]: contact.properties.hs_object_id,
      [propertyPrefix + "contact_status"]: contact.properties.hs_lead_status,
      [propertyPrefix + "contact_score"]:
        parseInt(contact.properties.hubspotscore) || 0,
    };

    const actionTemplate = {
      includeInAnalytics: 0,
      userId: contact.id,
      identity: contact.properties.email,
      userProperties: filterNullValuesFromObject(userProperties),
    };

    q.push({
      actionName: isCreated ? "Contact Created" : "Contact Updated",
      actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
      ...actionTemplate,
    });
  });
}

/**
 * The reference function for processing meetings through the HubSpot API. This is necessary as passing HubSpot's function as an argument to searchEngagement() doesn't work and generates an error. There may be a workaround for this, but for the sake of this test I didn't spend much time resolving that.
 *
 * @param searchObject {object} - the search object to pass to HubSpot's API
 * @returns {Promise<Void>}
 */
async function hs_processMeetings(searchObject) {
  return hubspotClient.crm.objects.meetings.searchApi.doSearch(searchObject);
}

/**
 * Process recent meetings through the HubSpot API. This function leverages a modular searchEngagement() function to handle the request as well as pagination and lastPulledDate logic.
 *
 * @param domain {object} - the domain object
 * @param hubId {string} - the hubId of the account to process
 * @param q {object} - the queue object which we are inheriting from the initial call and passing down to the callback function
 * @returns {Promise<Void>}
 */
async function processMeetings(domain, hubId, q) {
  await searchEngagement(
    "meetings",
    hs_processMeetings,
    undefined,
    processMeetingsCallback,
    domain,
    hubId,
    q
  );
}

/**
 * The callback function for processMeetings(). This function is called in between pages of results and is where you should act on meetings data to create and push actions.
 *
 * @param data {object[]} - the data to process
 * @param q {object} - the queue object which we are inheriting from the initial call and using to push actions to a queue.
 * @param lastPulledDate {Date} - the last time we pulled data from HubSpot
 * @returns {Promise<Void>}
 */
async function processMeetingsCallback(data, q, lastPulledDate) {
  const associatePerMeeting = {};
  const meetings = {};

  data.forEach((meeting) => {
    if (!meeting.properties) return;

    meetings[meeting.id] = {
      id: meeting.id,
      properties: meeting.properties,
      createdAt: meeting.createdAt,
      updatedAt: meeting.updatedAt,
    };
  });

  const meetingToContactsAssociationsResults =
    (
      await (
        await hubspotClient.apiRequest({
          method: "post",
          path: "/crm/v3/associations/MEETINGS/CONTACTS/batch/read",
          body: {
            inputs: Object.keys(meetings).map((meetingId) => ({
              id: meetingId,
            })),
            properties: ["email"],
          },
        })
      ).json()
    )?.results || [];

  meetingToContactsAssociationsResults.forEach((association) => {
    if (!association.from || !association.to) return;

    const meetingId = association.from.id;
    const contactId = association.to[0].id;

    associatePerMeeting[contactId] = meetingId;
  });

  const allContactIds = Object.keys(associatePerMeeting);

  // process all contacts at once, and then append their contact info to their respective meeting
  // instead of processing contacts PER meeting and wasting time
  const BatchReadInputSimplePublicObjectId = {
    properties: ["firstname", "lastname", "company", "email"],
    inputs: allContactIds.map((contactId) => ({ id: contactId })),
  };
  const contactAssociationsResults =
    await hubspotClient.crm.contacts.batchApi.read(
      BatchReadInputSimplePublicObjectId
    );
  contactAssociationsResults.results.forEach((contact) => {
    meetings[associatePerMeeting[contact.id]].contact = {
      id: contact.id,
      first_name: contact.properties.firstname,
      last_name: contact.properties.lastname,
      email: contact.properties.email,
    };
  });

  Object.values(meetings).forEach((meeting) => {
    const isCreated = new Date(meeting.createdAt) > lastPulledDate;

    const actionTemplate = {
      includeInAnalytics: 0,
      meetingId: meeting.id,
      meetingProperties: filterNullValuesFromObject(meeting.properties),
      meetingContact: filterNullValuesFromObject(meeting.contact),
    };

    q.push({
      actionName: isCreated ? "Meeting Created" : "Meeting Updated",
      actionDate: new Date(isCreated ? meeting.createdAt : meeting.updatedAt),
      ...actionTemplate,
    });
  });
}

// To support additional engagement types, all that is necessary is to mimic the 3 function structure that's used above for contacts, companies, and meetings. The searchEngagement() function will handle the rest.
// As an example, here's how you would add support for deals:

// async function hs_processDeals(...) { ... }
// async function processDeals(...) { ... }
// async function processDealsCallback(...) { ... }

const createQueue = (domain, actions) =>
  queue(async (action, callback) => {
    actions.push(action);

    if (actions.length > 2000) {
      console.log("inserting to HubSpot", {
        apiKey: domain.apiKey,
        count: actions.length,
      });

      const copyOfActions = _.cloneDeep(actions);
      actions.splice(0, actions.length);

      goal(copyOfActions);
    }

    callback();
  }, 100000000);

const drainQueue = async (domain, actions, q) => {
  if (q.length() > 0) await q.drain();

  if (actions.length > 0) {
    goal(actions);
  }

  return true;
};

const pullDataFromHubspot = async () => {
  console.log("start pulling data from HubSpot");

  const domain = await Domain.findOne({});

  for (const account of domain.integrations.hubspot.accounts) {
    account.lastPulledDate = account.lastPulledDate || new Date(0);
    if (!account.lastPulledDates) account.lastPulledDates = {};

    try {
      await refreshAccessToken(domain, account.hubId);
    } catch (err) {
      console.log(err, {
        apiKey: domain.apiKey,
        metadata: { operation: "refreshAccessToken" },
      });
    }

    const actions = [];
    const q = createQueue(domain, actions);

    try {
      await processContacts(domain, account.hubId, q);
      console.log("process contacts");
    } catch (err) {
      console.log(err, {
        apiKey: domain.apiKey,
        metadata: { operation: "processContacts", hubId: account.hubId },
      });
    }

    try {
      await processCompanies(domain, account.hubId, q);
      console.log("process companies");
    } catch (err) {
      console.log(err, {
        apiKey: domain.apiKey,
        metadata: { operation: "processCompanies", hubId: account.hubId },
      });
    }

    try {
      await processMeetings(domain, account.hubId, q);
      console.log("process meetings");
    } catch (err) {
      console.log(err, {
        apiKey: domain.apiKey,
        metadata: { operation: "processMeetings", hubId: account.hubId },
      });
    }

    try {
      await drainQueue(domain, actions, q);
      console.log("drain queue");
    } catch (err) {
      console.log(err, {
        apiKey: domain.apiKey,
        metadata: { operation: "drainQueue", hubId: account.hubId },
      });
    }

    await saveDomain(domain);

    console.log("finish processing account");
  }
};

module.exports = pullDataFromHubspot;
