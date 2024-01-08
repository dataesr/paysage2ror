//  node --experimental-specifier-resolution=node paysage2ror.mjs
import axios from 'axios';
import axiosRetry from 'axios-retry';
import { createObjectCsvWriter } from 'csv-writer';
import 'dotenv/config';

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
        paysageNames: [...new Set([structure?.displayName, structure?.currentName?.officialName, structure?.currentName?.usualName].filter((x) => x))],
      })
    }
  });
  if (response.data.data.length === PAYSAGE_PAGE_SIZE) {
    const newStructures = await getStructuresWithoutRor(page + 1);
    structures = [...structures, ...newStructures];
  }
  return structures;
};

const getRors = async (structures) => {
  const rorStructures = []
  for (let i = 0; i < structures.length; i++) {
    const structure = structures[i];
    for (let j = 0; j < structure.paysageNames.length; j++) {
      const query = structure.paysageNames[j];
      const response = await axios.get(`https://api.ror.org/organizations?affiliation=${encodeURI(query)}`);
      const result = (response?.data?.items ?? []).find((item) => item?.chosen);
      if (result) {
        structure.ror = result?.organization?.id;
        structure.rorName = result?.organization?.name;
        rorStructures.push(structure);
        break;
      };
    };
  };
  return rorStructures;
};

const writeCsv = (data) => {
  const csvWriter = createObjectCsvWriter({
    path: 'paysage2ror.csv',
    header: [
      { id: 'paysageId', title: 'ID de la structure' },
      { id: 'paysageUrl', title: 'URL Paysage' },
      { id: 'paysageNames', title: 'Noms Paysage' },
      { id: 'ror', title: 'RoR' },
      { id: 'rorName', title: 'Noms RoR' },
      { id: 'type', title: "Type d'identifiant" },
      { id: 'header1', title: 'Valeur' },
      { id: 'header2', title: 'Date de début {2020-07-02}' },
      { id: 'header3', title: 'Date de fin {2020-07-02}' },
      { id: 'header4', title: 'Actif {O = Oui, N = Non}' },
    ],
  });

  const modifiedData = data.map((record) => {
    return {
      ...record,
      type: 'ror',
      header1: record.ror.replace('https://ror.org/', ''),
      header2: '', // Empty
      header3: '', // Empty
      header4: 'O', // O, because all of them are supposed to be activ
    };
  });

  return csvWriter.writeRecords(modifiedData);
};


// Collect all French structures from Paysage without RoR identifier
console.log('01 _ Collect structures from Paysage');
const structuresWithoutRor = await getStructuresWithoutRor();
// Check with our Affiliation Matcher if there is a RoR
console.log('02 _ For structures, guess RoR from RoR API');
const structures = await getRors(structuresWithoutRor);
const structuresConsolidatedWithRor = structures.filter((structure) => structure?.ror);
// Write results into CSV
console.log('03 _ Write results in CSV');
await writeCsv(structuresConsolidatedWithRor);
console.log('Done ! Output ready for bulk import !');
