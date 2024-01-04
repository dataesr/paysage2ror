//  node --experimental-specifier-resolution=node --env-file=.env paysage2ror.mjs
import axios from 'axios';
import axiosRetry from 'axios-retry';
import { createObjectCsvWriter } from 'csv-writer';
import 'dotenv/config';

const PAYSAGE_PAGE_LIMIT = 10; // Set to 0 for no limit
const PAYSAGE_PAGE_SIZE = 200;

axiosRetry(axios, {
  retries: 5,
  retryDelay: (retryCount) => {
    console.log(`retry attempt: ${retryCount}`);
    return retryCount * 3 * 60 * 1000;
  },
});

const getStructuresWithoutRor = async (page = 1) => {
  console.log(`Page ${page} of Paysage`);
  const skip = (page - 1) * PAYSAGE_PAGE_SIZE;
  // Retrieve all structures from the geographical category "France"
  let structures = [];
  const response = await axios.get(`https://api.paysage.dataesr.ovh/geographical-categories/4d6le/structures?limit=${PAYSAGE_PAGE_SIZE}&skip=${skip}`, { headers: { 'X-API-KEY': process.env.XAPIKEY } });
  (response?.data?.data ?? []).forEach((structure) => {
    const structureRors = (structure?.identifiers ?? []).filter((identifier) => identifier.type === 'ror');
    if (structureRors.length === 0) {
      structures.push({
        paysageId: structure.id,
        paysageUrl: `https://paysage.enseignementsup-recherche.gouv.fr/structures/${structure.id}/presentation`,
        paysageNames: [structure?.displayName, structure?.currentName?.officialName, structure?.currentName?.usualName].filter((x) => x),
      })
    }
  });
  if ((PAYSAGE_PAGE_LIMIT === 0 || page < PAYSAGE_PAGE_LIMIT) && (response.data.data.length === PAYSAGE_PAGE_SIZE)) {
    const str = await getStructuresWithoutRor(page + 1);
    structures = [...structures, ...str];
  }
  return structures;
};

const getRors = (structures) => {
  return Promise.all(structures.map(async (structure) => {
    for (let i = 0; i < structure.paysageNames.length; i++) {
      const query = structure.paysageNames[i];
      await new Promise(resolve => setTimeout(resolve, 30 * 1000));
      const response = await axios.get(`https://api.ror.org/organizations?affiliation=${encodeURI(query)}`);
      const result = (response?.data?.items ?? []).find((item) => item?.chosen);
      if (result) {
        structure.ror = result?.organization?.id;
        structure.rorName = result?.organization?.name;
        return structure;
      };
    };
    return structure;
  }));
};

const writeCsv = (data) => {
  const csvWriter = createObjectCsvWriter({
    path: 'output.csv',
    header: [
      { id: 'paysageId', title: 'paysageId' },
      { id: 'paysageUrl', title: 'paysageUrl' },
      { id: 'paysageNames', title: 'paysageNames' },
      { id: 'ror', title: 'ror' },
      { id: 'rorName', title: 'rorName' },
    ]
  });
  return csvWriter.writeRecords(data);
}

// Collect all French structures from Paysage without RoR identifier
console.log('01 _ Collect structures from Paysage')
const structuresWithoutRor = await getStructuresWithoutRor();
// Check with our Affiliation Matcher if there is a RoR
console.log('02 _ For structures, guess RoR from RoR API')
const structures = await getRors(structuresWithoutRor);
const structuresConsolidatedWithRor = structures.filter((structure) => structure?.ror);
// Write results into CSV
console.log('03 _ Write results in CSV')
await writeCsv(structuresConsolidatedWithRor);
console.log('Done !');
