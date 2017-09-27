const fs = require('fs')
const path = require('path')
const request = require('request')
const H = require('highland')
const R = require('ramda')
const shapefile = require('shapefile')
const extract = require('extract-zip')
const normalizer = require('@spacetime/nyc-street-normalizer')

const sourceUrl = 'https://github.com/nypl-spacetime/nyc-historical-streets/archive/master.zip'
const extractDir = 'nyc-historical-streets-master'

function getFeatures (dataDir, layer, callback) {
  const year = parseInt(layer.year)
  const layerId = String(layer.id)

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
    // If feature has no name, group this feature in 'error' group
    return 'error'
  }

  const name = feature.properties.name.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]+/g, '')
  return `${feature.properties.layerId}-${name}`
}

function transformGroup (group) {
  let objects = []

  const id = group.group
  const feature = group.features[0]

  if (id === 'error') {
    objects.push({
      type: 'log',
      obj: {
        error: `Feature without name encountered in layer ${feature.properties.layerId}`
      }
    })

    return objects
  }

  const getCoordinates = (feature) => feature.geometry && feature.geometry.coordinates

  const noCoordinates = group.features
    .filter((feature) => !getCoordinates(feature))

  if (noCoordinates.length) {
    objects.push({
      type: 'log',
      obj: {
        error: `Feature without coordinates encountered in layer ${feature.properties.layerId}: ${feature.properties.name}`
      }
    })
  }

  const year = feature.properties.year

  let geometry
  if (group.features.length === 1) {
    geometry = feature.geometry
  } else {
    geometry = {
      type: 'MultiLineString',
      coordinates: group.features
        .filter(getCoordinates)
        .map(getCoordinates)
    }
  }

  let mainId = id
  let mainName = feature.properties.name
  const multiple = /(.*) \((.*)\)/.exec(mainName)

  if (multiple) {
    mainId = `${id}-1`
    mainName = multiple[1]
  }

  const layerId = feature.properties.layerId

  objects = objects.concat([{
    type: 'object',
    obj: {
      id: mainId,
      name: normalizer(mainName),
      type: 'st:Street',
      validSince: year,
      validUntil: year,
      data: {
        layerId,
        originalName: mainName
      },
      geometry
    }
  }, {
      type: 'relation',
      obj: {
        from: mainId,
        to: `mapwarper/layer-${layerId}`,
        type: 'st:in'
      }
  }])

  if (multiple) {
    secondId = `${id}-2`
    secondName = multiple[2]

    objects = [...objects, {
      type: 'object',
      obj: {
        id: secondId,
        name: normalizer(secondName),
        type: 'st:Street',
        validSince: year,
        validUntil: year,
        data: {
          layerId: feature.properties.layerId,
          originalName: secondName
        }
      }
    }, {
      type: 'relation',
      obj: {
        from: secondId,
        to: mainId,
        type: 'st:sameAs'
      }
    }]
  }

  return objects
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
  const layers = require(path.join(dataDir, 'layers.json'))

  H(layers)
    .filter((layer) => fs.existsSync(path.join(dataDir, String(layer.id))))
    .map(H.curry(getFeatures, dataDir))
    .nfcall([])
    .series()
    .flatten()
    .group(layerIdAndName)
    .map((groups) => Object.keys(groups)
      .map((group) => ({
        group,
        features: groups[group]
      })
    ))
    .sequence()
    .map(transformGroup)
    .flatten()
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

