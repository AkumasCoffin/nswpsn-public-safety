// Fleet taxonomy — the states and department categories a fleet vehicle can
// belong to, shared by fleet-compose.html (the pickers + conditional spec
// sections) and wire.html's Fleet tab (filters + labels).
//
// `cat` drives the conditional compose sections:
//   fire      -> water tank / foam tank / CAFS / BA sets
//   ambulance -> stretchers
// Forestry Corporation NSW and NPWS count as fire — they run firefighting
// fleets. The backend stores agency + category as plain strings; this file
// is the single place the lists live.
(function () {
  "use strict";

  const departments = {
    NSW: [
      { name: "NSW Police Force", cat: "police" },
      { name: "Fire and Rescue NSW (FRNSW)", cat: "fire" },
      { name: "NSW Rural Fire Service (RFS)", cat: "fire" },
      { name: "NSW State Emergency Service (SES)", cat: "ses" },
      { name: "NSW Ambulance", cat: "ambulance" },
      { name: "Marine Rescue NSW", cat: "marine" },
      { name: "VRA Rescue NSW", cat: "rescue" },
      { name: "Forestry Corporation NSW", cat: "fire" },
      { name: "National Parks and Wildlife Service", cat: "fire" },
      { name: "Water NSW", cat: "other" },
    ],
    VIC: [
      { name: "Victoria Police", cat: "police" },
      { name: "Fire Rescue Victoria", cat: "fire" },
      { name: "Country Fire Authority", cat: "fire" },
      { name: "Victoria State Emergency Service", cat: "ses" },
      { name: "Ambulance Victoria", cat: "ambulance" },
    ],
    QLD: [
      { name: "Queensland Police Service", cat: "police" },
      { name: "Queensland Fire Department", cat: "fire" },
      { name: "Queensland SES", cat: "ses" },
      { name: "Queensland Ambulance Service", cat: "ambulance" },
    ],
    WA: [
      { name: "WA Police Force", cat: "police" },
      { name: "Department of Fire and Emergency Services (DFES)", cat: "fire" },
      { name: "St John Ambulance WA", cat: "ambulance" },
      { name: "Volunteer Marine Rescue WA", cat: "marine" },
    ],
    SA: [
      // MFS, CFS, SASES (and SAAS alongside) sit under the SA Fire and
      // Emergency Services Commission (SAFECOM); listed individually since a
      // vehicle belongs to a service, not the commission.
      { name: "SA Police", cat: "police" },
      { name: "SA Metropolitan Fire Service (MFS)", cat: "fire" },
      { name: "SA Country Fire Service (CFS)", cat: "fire" },
      { name: "SA State Emergency Service (SASES)", cat: "ses" },
      { name: "SA Ambulance Service (SAAS)", cat: "ambulance" },
    ],
    TAS: [
      { name: "Tasmania Police", cat: "police" },
      { name: "Tasmania Fire Service (TFS)", cat: "fire" },
      { name: "Tasmania State Emergency Service", cat: "ses" },
      { name: "Ambulance Tasmania", cat: "ambulance" },
    ],
    ACT: [
      { name: "ACT Police", cat: "police" },
      { name: "ACT Fire and Rescue", cat: "fire" },
      { name: "ACT Rural Fire Service", cat: "fire" },
      { name: "ACT State Emergency Service", cat: "ses" },
      { name: "ACT Ambulance Service", cat: "ambulance" },
      { name: "Southcare", cat: "ambulance" },
    ],
    NT: [
      { name: "NT Police", cat: "police" },
      { name: "NT Fire and Rescue Service", cat: "fire" },
      { name: "NT State Emergency Service", cat: "ses" },
      { name: "St John Ambulance NT", cat: "ambulance" },
    ],
  };

  window.FleetVocab = {
    states: Object.keys(departments),
    departments,
    categoryOf(state, agencyName) {
      const list = departments[state] || [];
      const hit = list.find((d) => d.name === agencyName);
      return hit ? hit.cat : null;
    },
  };
})();
