const fs = require('fs')
const path = require('path')
const H = require('highland')
const R = require('ramda')
const shapefile = require('shapefile')

const layers = require('./data/layers.json').layers

function getFeatures (layer, callback) {
  const year = parseInt(layer.year)
  const layerId = String(layer.external_id)

  let features = []

  shapefile.open(path.join(__dirname, 'data', layerId, `${layerId}.shp`))
    .then((source) => source.read()
      .then(function log (result) {
        if (result.done) {
          callback(null, features)
          return
        }

        const feature = result.value

        features.push({
          type: 'Feature',
          properties: {
            name: feature.properties.name,
            year,
            layerId,
          },
          geometry: feature.geometry
        })

        return source.read().then(log)
      }))
    .catch(callback)
}

function layerIdAndName (feature) {
  if (!feature.properties.name) {
    console.error(`Feature without name encountered in layer ${feature.properties.layerId}`)
    return
  }

  const name = feature.properties.name.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]+/g, '')
  return `${feature.properties.layerId}-${name}`
}

function transformGroup (features) {

  const feature = features[0]
  const name = feature.properties.name
  const year = feature.properties.year

  return {
    type: 'object',
    obj: {
      id: layerIdAndName(feature),
      name: name,
      type: 'st:Street',
      validSince: year,
      validUntil: year,
      data: {
        layerId: feature.properties.layerId
      },
      geometry: {
        type: 'MultiLineString',
        coordinates: features.map(function (f) {
          return f.geometry.coordinates
        })
      }
    }
  }
}

function transform (config, dirs, tools, callback) {
  H(layers)
    .filter((layer) => fs.existsSync(path.join(__dirname, 'data', String(layer.external_id))))
    .map(H.curry(getFeatures))
    .nfcall([])
    .series()
    .flatten()
    .filter(layerIdAndName)
    .group(layerIdAndName)
    .map(R.values)
    .sequence()
    .map(transformGroup)
    .map(H.curry(tools.writer.writeObject))
    .nfcall([])
    .series()
    .stopOnError(callback)
    .done(callback)


//       H(R.values(groups))
//         .map(convertFeatures)
//         .compact()
//         .flatten()
//         .map(H.curry(tools.writer.writeObject))
//         .nfcall([])
//         .series()
//         .stopOnError(callback)
//         .done(callback)

}

// ==================================== API ====================================

module.exports.steps = [
  transform
]



// 'use strict'
// var fs = require('fs')
// var path = require('path')
// var crypto = require('crypto')
// var H = require('highland')
// var R = require('ramda')
// var JSONStream = require('JSONStream')

// var files = [
//   'manhattan',
//   'brooklyn'
// ]

// function streetName (feature) {
//   return feature.properties.streetname
// }

// function convertFeatures (features) {
//   var ids = features.map(function (f) {
//     return f.properties.id
//   }).join(',')
//   var id = crypto.createHash('md5').update(ids).digest('hex')
//   var name = streetName(features[0])

//   return {
//     type: 'object',
//     obj: {
//       id: id,
//       name: name,
//       type: 'st:Street',
//       validSince: 1857,
//       validUntil: 1862,
//       geometry: {
//         type: 'MultiLineString',
//         coordinates: features.map(function (f) {
//           return f.geometry.coordinates
//         })
//       }
//     }
//   }
// }

// function transform (config, dirs, tools, callback) {
//   var streams = files
//     .map((file) => `./data/${file}.geojson`)
//     .map((filename) => fs.createReadStream(path.join(__dirname, filename))
//       .pipe(JSONStream.parse('features.*')))

//   // Some streets have no street name...
//       //   For example, in Manhttan: 337, 940, 1106, 1394, 2058, 2662, 2702

//   H(streams)
//     .map((stream) => H(stream))
//     .merge()
//     .filter(streetName)
//     .group(streetName)
//     .toArray((groupsArray) => {
//       var groups = groupsArray[0]
//       H(R.values(groups))
//         .map(convertFeatures)
//         .compact()
//         .flatten()
//         .map(H.curry(tools.writer.writeObject))
//         .nfcall([])
//         .series()
//         .stopOnError(callback)
//         .done(callback)
//     })
// }

// // ==================================== API ====================================

// module.exports.steps = [
//   transform
// ]
