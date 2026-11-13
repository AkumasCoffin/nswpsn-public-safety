/**
 * The distributors' network area files (13 November 2026), every ring cut
 * down to a couple of dozen of its points; the wrapping around the points is
 * as each distributor writes it.
 */

/** Ausgrid: one ring of {lat, lng}, not closed. */
export const AUSGRID_AREA = [
  { lat: -33.08904, lng: 150.85149 }, { lat: -33.11266, lng: 151.13096 }, { lat: -33.34995, lng: 151.03225 },
  { lat: -33.49081, lng: 151.01916 }, { lat: -33.70412, lng: 151.0144 }, { lat: -33.83157, lng: 151.0361 },
  { lat: -33.90724, lng: 150.97323 }, { lat: -34.12556, lng: 150.92806 }, { lat: -33.87412, lng: 151.2852 },
  { lat: -33.06572, lng: 151.66091 }, { lat: -32.61687, lng: 151.95826 }, { lat: -32.61898, lng: 151.59429 },
  { lat: -32.29019, lng: 151.36776 }, { lat: -32.01074, lng: 151.4416 }, { lat: -31.84994, lng: 151.57732 },
  { lat: -31.57217, lng: 151.49461 }, { lat: -31.56073, lng: 151.33623 }, { lat: -31.84717, lng: 151.18666 },
  { lat: -31.84354, lng: 150.59668 }, { lat: -31.80435, lng: 150.18172 }, { lat: -32.3375, lng: 150.14638 },
  { lat: -32.50576, lng: 150.2428 }, { lat: -32.88772, lng: 150.34011 }, { lat: -33.06615, lng: 150.59119 },
];

const CITIPOWER_RING = [
  [144.975767, -37.869448], [145.029092, -37.865791], [145.053467, -37.84555], [145.09874, -37.824569],
  [145.107724, -37.792418], [145.104704, -37.789888], [145.077992, -37.776876], [145.067257, -37.783533],
  [145.05489, -37.781078], [145.047965, -37.780175], [145.04467, -37.783989], [145.018079, -37.786222],
  [145.017882, -37.7974], [145.008734, -37.796292], [145.005493, -37.785625], [145.029632, -37.764601],
  [145.031776, -37.757196], [144.980001, -37.756847], [144.933297, -37.753081], [144.934569, -37.756697],
  [144.93507, -37.760357], [144.917375, -37.792808], [144.905955, -37.810972], [144.913234, -37.841979],
  [144.975767, -37.869448],
];

/** CitiPower, and Powercor in the same shape: a FeatureCollection of one Polygon. */
export const CITIPOWER_AREA = {
  type: 'FeatureCollection',
  features: [{ type: 'Feature', properties: { name: 'Citipower' }, geometry: { type: 'Polygon', coordinates: [CITIPOWER_RING] } }],
};
export const POWERCOR_AREA = {
  type: 'FeatureCollection',
  features: [
    {
      type: 'Feature',
      properties: { name: 'Powercor' },
      // Four points at the corners of Powercor's own ring: western Victoria.
      geometry: { type: 'Polygon', coordinates: [[[144.592542, -38.004412], [140.962247, -38.057], [140.9634, -33.981352], [146.215479, -36.1], [144.592542, -38.004412]]] },
    },
  ],
};

const JEMENA_PROPS = { exportdate: '20201005_1619', type: 'boundary' };
export const JEMENA_RING = [
  [144.89733280678485, -37.83452137966782], [144.9029793660217, -37.85722570069774], [144.90987186534133, -37.87019793715881],
  [144.88438433641642, -37.86767405413229], [144.87093606691383, -37.8612789094399], [144.84926570692997, -37.75681527759827],
  [144.83053317544577, -37.72574545657977], [144.83904114660038, -37.71529523406767], [144.82137776564682, -37.69912551667352],
  [144.67784563473057, -37.675150202621296], [144.8437138890253, -37.5015194935234], [144.8569820799849, -37.526838170228295],
  [144.8666324582838, -37.51185485718793], [144.88271833781187, -37.50285305509229], [144.963997320736, -37.629664894981396],
  [144.97201306690576, -37.66300015983682], [145.1110962462341, -37.72151469766767], [145.1032615077048, -37.74024644948685],
  [145.07785019830385, -37.75435161597058], [145.0686746982437, -37.77617967182085], [145.04068962442398, -37.7854241686799],
  [145.0153259359809, -37.80166754485767], [145.029404491377, -37.75904525245259], [144.9356590884087, -37.76976757565449],
  [144.89733280678485, -37.83452137966782],
];
/** A pocket inside the area that Jemena does not supply, whole. */
export const JEMENA_POCKET = [
  [144.92315323171135, -37.789235034508664], [144.92523587428536, -37.787852402532295], [144.92875711477816, -37.788220826405244],
  [144.92886878224726, -37.788695888765616], [144.92491289924664, -37.79107743821954], [144.92315323171135, -37.789235034508664],
];
/** The whole world, as Jemena's first polygon draws it. */
export const JEMENA_WORLD = [[0, 90], [180, 90], [180, -90], [0, -90], [-180, -90], [-180, 0], [-180, 90], [0, 90]];

/** Jemena: the world with the supply area cut out, then a polygon per pocket. */
export const JEMENA_AREA = {
  type: 'FeatureCollection',
  features: [
    { type: 'Feature', properties: JEMENA_PROPS, geometry: { type: 'Polygon', coordinates: [JEMENA_WORLD, JEMENA_RING] } },
    { type: 'Feature', properties: JEMENA_PROPS, geometry: { type: 'Polygon', coordinates: [JEMENA_POCKET] } },
  ],
};

/** Energex (and Ergon Energy, the same publisher): an ArcGIS export of one Polygon. */
export const ENERGEX_AREA = {
  type: 'FeatureCollection',
  crs: { type: 'name', properties: { name: 'EPSG:4326' } },
  features: [
    {
      type: 'Feature',
      id: 4,
      geometry: {
        type: 'Polygon',
        coordinates: [[
          [154.057074670308, -25.7540296638825], [153.251165498781, -28.2468504609635], [153.140634614995, -28.3109864414761],
          [152.945511783897, -28.3354681212995], [152.780072334176, -28.3466076840604], [152.619377645649, -28.2839069416122],
          [152.529463282687, -28.292945953487], [152.411680782007, -28.1105994690675], [152.369801264173, -27.9621171851151],
          [152.152060344525, -27.8739670813532], [152.065519217438, -27.7245972212563], [152.009705182037, -27.5780310170104],
          [152.199739823314, -27.3336827312922], [152.168900064027, -27.2085547075164], [152.179199661086, -27.0887148270801],
          [152.165572577552, -26.9695547364359], [152.155523545778, -26.7645012877485], [152.080411702018, -26.6575090952795],
          [152.241056658912, -26.4857003142846], [152.389581506467, -26.4055139590773], [152.483022379453, -26.2258223272083],
          [152.710719140544, -26.0347785330201], [153.010396103332, -25.866832506932], [154.057074670308, -25.7540296638825],
        ]],
      },
      properties: { Id: 0, InPoly_FID: 0, MaxSimpTol: 20, MinSimpTol: 20, OBJECTID: 4, Region: 'SOUTH-EAST', Shape__Area: 67097494801.4844, Shape__Length: 1343290.97904812, SimPgnFlag: 0 },
    },
  ],
};

/** Each distributor's file, by the URL it is read from. */
export const AREA_FILES: Readonly<Record<string, unknown>> = {
  'https://www.ausgrid.com.au/api/outages-map/network-area': AUSGRID_AREA,
  'https://www.powercor.com.au/assets/geo/citipower-network.json': CITIPOWER_AREA,
  'https://www.powercor.com.au/assets/geo/powercor-network.json': POWERCOR_AREA,
  'https://poweroutages.jemena.com.au/boundary.json': JEMENA_AREA,
  'https://www.energex.com.au/static/PRD/ex_map_servicearea.geojson': ENERGEX_AREA,
  'https://www.ergon.com.au/static/PRD/ee_map_servicearea.geojson': ENERGEX_AREA,
};
