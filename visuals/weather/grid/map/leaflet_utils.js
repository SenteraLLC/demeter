
colors = {
    "North American Zone (NAZ)": "red",
    "Middle American Zone (MAZ)": "blue",
    "South American Zone (SAZ)": "deepskyblue",
    "European Zone (EUR)": "green",
    "Asia Pacific Zone (APAC)": "orange",
    "African Zone (Africa)": "magenta"
}

// get legend color 
var getLegendLabels = function () {
    var labels = ['<b>Zones</b>'];
    for (var key in colors) {
        labels.push('<i style="background:' + get_color_for_zone(key) + '"></i> ' + key);
    }
    return labels.join('<br>')
}

// get specified color for an ABI zone 
var get_color_for_zone = function (zone) {
    return colors[zone]
}

// highlight a feature with thicker line and greater opacity 
var highlightFeature = function (e, op) {
    var layer = e.target;
    layer.setStyle({
        weight: 3,
        fillOpacity: op
    })
}

// reset a layer to its original style
var resetLayer = function (e, layer) {
    layer.resetStyle(e.target);
}

// // show field pop-up with id and planting information 
// var add_field_popup = function (feature, layer) {
//     layer.bindPopup(
//         '<p> <b>Farm</b>: ' + feature.properties.farm + '</p>' +
//         '<p> <b>Field ID</b>: ' + feature.properties.field_id + '</p>' +
//         '<p> <b>Planting Date</b>: ' + feature.properties.plant_date + '</p>' +
//         '<p> <b>Variety</b>: ' + feature.properties.variety + '</p>')
// }

// creates zone point layer, selecting all fields that have passed ``zone_name``
var zone_points_layer = function (zone_name) {
    var layer = L.geoJson(ABI_FIELDS, {
        style: {
            color: get_color_for_zone(zone_name)
        },
        filter: function (feature, layer) {
            return (feature.properties.zone === zone_name)
        },
        onEachFeature: function (feature, layer) {
            layer.on({
                mouseover: function (e) {
                    info.update('Field', feature)
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

// remove layer after zoom is achieved
var remove_layer_when_zoom = function (thisLayer, limit) {
    if (map.getZoom() > limit && map.hasLayer(thisLayer)) {
        map.removeLayer(thisLayer);
    }
    if (map.getZoom() < limit && map.hasLayer(thisLayer) == false) {
        map.addLayer(thisLayer);
    }
}

// conditonal statements for pop up information based on layer type 
var text_for_layer = function (layer_type, feature) {
    if (layer_type == 'Default') {
        return '<p> Hover over a field of interest.</p><p> Click to zoom to feature. </p>'
    }
    if (layer_type == 'Field') {
        return '<p> <b>Farm</b>: ' + feature.properties.organization + '</p>' +
            '<p> <b>Field ID</b>: ' + feature.properties.field_name + '</p>' +
            '<p> <b>Planting Date</b>: ' + feature.properties.plant_date + '</p>' +
            '<p> <b>Variety</b>: ' + feature.properties.variety + '</p>'
    }
    // if (layer_type == 'Zone') {
    //     return '<p> <b>Zone/b>: ' + feature.properties.zone + '</p>'
    // }
}

// add histogram (in development)
var add_histogram = function (
    data,
    div_name,
    plot_variable = 'plant_date',
    plot_axis_name = 'Planting Date',
    barmode = 'overlay',
    overlayChange = true
) {
    /*
    Create histogram of ``plot_variable`` (as shown in ``data``) organized by ABI region for now 
    (i.e., color is decided by ``color_for_zone`` function). 

    Some development caveats: 
    - ``dict`` parameter is only used to parse region names
    - added boolean parameter to note whether or not we need to pay attention to loaded overlays, 
        but I didn't develop code for histograms where ``overlayChange`` == false
    */

    var chartDiv = document.getElementById(div_name);

    // organize data
    var x = [];
    var color = [];
    var zone = [];

    data.forEach(function (feature, i) {
        x.push(feature.properties[plot_variable]);
        color.push(get_color_for_zone(feature.properties["zone"]));
        zone.push(feature.properties["zone"]);
    })

    current_zones = [... new Set(zone)];
    var plot_data = [];
    current_regions.forEach(function (r) {
        var this_data = {
            x: x.filter((item, i) => zone[i] == r),
            name: r,
            opacity: 0.2,
            marker: { color: get_color_for_zone(r) },
            type: 'histogram'
        }
        plot_data.push(this_data);
    });

    var layout = {
        showlegend: true,
        autosize: false,
        width: chartDiv.clientWidth,
        height: chartDiv.clientHeight,
        barmode: barmode,
        xaxis: {
            title: plot_axis_name
        },
        yaxis: {
            title: "Count"
        }
    };
    Plotly.newPlot(chartDiv, plot_data, layout)

    return plot_data
}

var add_statistics = function (data, div_name, plot_variable = 'plant_date') {

    var statsDiv = document.getElementById(div_name);

    var x_values = [];
    data.forEach(function (feature, i) {
        x_values.push(feature.properties[plot_variable]);
    });

    var x_dates = x_values.map(date => new Date(date));
    x_dates.sort(function (a, b) {
        return a - b;
    });

    const minDate = x_dates[0];
    const maxDate = x_dates[x_dates.length - 1];
    const medDate = x_dates[Math.ceil(x_dates.length / 2)];

    statsDiv.innerHTML = '<p><b>Number of fields</b>: ' + x_dates.length + '<br>' + '<br>' +
        '<b>Minimum</b>: ' + minDate.toDateString() +
        '&emsp;&emsp;<b>Median</b>: ' + medDate.toDateString() +
        '&emsp;&emsp;<b>Maximum</b>: ' + maxDate.toDateString() + '</p>'

    return x_dates

}

var get_displayed_features = function (layer_group) {
    var current_features = [];
    var map_bounds = map.getBounds();

    // if data is in bounds and in current layer, keep for plotting and summarization
    layer_group.eachLayer(function (layer) {
        let zone = layer.feature.properties['zone'];
        let bounds = layer.getBounds();
        if (map.hasLayer(overlays[zone]) && map_bounds.contains(bounds)) {
            current_features.push(layer.feature);
        }
    });

    return current_features;
}