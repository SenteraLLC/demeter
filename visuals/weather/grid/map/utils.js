// conditonal statements for info box based on layer type 
var text_for_layer = function (layer_type, feature) {
    if (layer_type == 'Default') {
        return '<p> Hover over a layer for information.</p><p> Click to zoom to next layer. </p>'
    }
    if (layer_type == 'World UTM') {
        return '<p> <b>UTM Zone</b>: ' + feature.properties.zone + '</p>' +
            '<p> <b>UTM Row</b>: ' + feature.properties.row + '</p>'
    }
    if (layer_type == 'Raster') {
        return '<p> <b>Cell ID</b>: ' + feature.properties.cell_id + '</p>'
    }
}

// creates world UTM polygon layer
var add_world_utm_polygons = function () {
    var layer = L.geoJson(WORLD_UTM, {
        style: {
            fillColor: 'black',
            opacity: 0.5,
            fillOpacity: 0.2,
            color: 'black'
        },
        onEachFeature: function (feature, layer) {
            layer.on({
                mouseover: function (e) {
                    info.update('World UTM', feature)
                },
                click: function (e) {
                    map.fitBounds(e.target.getBounds());
                },
                mouseout: function (e) {
                    info.update('Default')
                }
            });
        }
    });
    return layer
}

// add raster layer
var add_raster_layer = function () {
    var layer = L.geoJson(RASTER, {
        style: {
            fillColor: 'black',
            opacity: 0.5,
            fillOpacity: 0.05,
            color: 'black',
            weight: 0.75
        },
        onEachFeature: function (feature, layer) {
            layer.on({
                mouseover: function (e) {
                    info.update('Raster', feature)
                },
                click: function (e) {
                    map.fitBounds(e.target.getBounds());
                },
                mouseout: function (e) {
                    info.update('Default')
                }
            });
        }
    });
    return layer
}

// button triggers
var zoomToUTM = function () {

    if (!map.hasLayer(world_utm)) {
        map.removeLayer(raster);
        map.addLayer(world_utm);
    }
    map.panTo([0, 0]);
    map.setZoom(2);

    var lat = document.getElementById("lat").value;
    var long = document.getElementById("long").value;
    var pt = new L.latLng(lat, long);

    if (map.hasLayer(marker)) {
        map.removeLayer(marker);
    }
    marker = new L.marker([lat, long], { color: 'black' }).addTo(map);

    world_utm.resetStyle();
    world_utm.eachLayer(function (layer) {
        let bounds = layer.getBounds();
        if (bounds.contains(pt)) {
            layer.setStyle({ fillColor: 'red', fillOpacity: 0.8 });
        }
        else {
            layer.setStyle({ opacity: 0.1 });
        }
    });

    // get_raster(zone, row);
}

var highlightCell = function () {

    if (!map.hasLayer(world_utm)) {
        map.addLayer(world_utm);
    }
    if (!map.hasLayer(raster)) {
        map.addLayer(raster);
    }
    map.flyTo(raster.getBounds().getCenter(), 6);
    // map.fitBounds(raster.getBounds());

    var lat = document.getElementById("lat").value;
    var long = document.getElementById("long").value;
    var pt = new L.latLng(lat, long);

    if (map.hasLayer(marker)) {
        map.removeLayer(marker);
    }
    marker = new L.marker([lat, long]).addTo(map);

    world_utm.resetStyle();
    world_utm.setStyle({ opacity: 0.1 });
    raster.resetStyle();
    raster.eachLayer(function (layer) {
        let bounds = layer.getBounds();
        if (bounds.contains(pt)) {
            layer.setStyle({ fillColor: 'red', fillOpacity: 0.8 });
        }
    });
}


var zoomToCell = function () {

    if (map.hasLayer(world_utm)) {
        map.removeLayer(world_utm);
        map.addLayer(raster);
    }
    map.fitBounds(raster.getBounds());

    var lat = document.getElementById("lat").value;
    var long = document.getElementById("long").value;
    var pt = new L.latLng(lat, long);

    if (map.hasLayer(marker)) {
        map.removeLayer(marker);
    }
    marker = new L.marker([lat, long]).addTo(map);

    raster.resetStyle();
    raster.eachLayer(function (layer) {
        let bounds = layer.getBounds();
        if (bounds.contains(pt)) {
            layer.setStyle({ fillColor: 'red', fillOpacity: 0.5 });
            map.flyTo([lat, long], 13);

        }
    });
}



// var get_raster_to_js = function (zone, row) {


// }