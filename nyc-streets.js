const fs = require('fs')
const path = require('path')
const request = require('request')
const H = require('highland')
const R = require('ramda')
const shapefile = require('shapefile')
const extract = require('extract-zip')

const sourceUrl = 'https://github.com/nypl-spacetime/nyc-historical-streets/archive/master.zip'
const extractDir = 'nyc-historical-streets-master'

function getFeatures (dataDir, layer, callback) {
  const year = parseInt(layer.year)
  const layerId = String(layer.external_id)

  let features = []

  shapefile.open(path.join(dataDir, layerId, `${layerId}.shp`))
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

function download (config, dirs, tools, callback) {
  const zipPath = path.join(dirs.current, 'data.zip')

  request(sourceUrl)
    .pipe(fs.createWriteStream(zipPath))
    .on('finish', () => {
      extract(zipPath, {
        dir: dirs.current
      }, (err) => {
        if (err) {
          callback(err)
          return
        }

        callback()
      })
    })
}

function transform (config, dirs, tools, callback) {
  const dataDir = path.join(dirs.previous, extractDir)
  const layers = require(path.join(dataDir, 'layers.json')).layers

  H(layers)
    .filter((layer) => fs.existsSync(path.join(dataDir, String(layer.external_id))))
    .map(H.curry(getFeatures, dataDir))
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
}

// ==================================== API ====================================

module.exports.steps = [
  download,
  transform
]

