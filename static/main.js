L.mapbox.accessToken = 'pk.eyJ1IjoicGV0ZXNoYWRib2x0IiwiYSI6Il9wNTQxNUUifQ.yZTAWXMJZVR29WM5O7ODbA';
var map = L.mapbox.map('map', 'peteshadbolt.dbdd760e').setView([54.5, -2], 6);

// Prepare the request
var xhr = new XMLHttpRequest();
xhr.onreadystatechange=function() {
    if (xhr.readyState==4 && xhr.status==200) {
        add_polygon(JSON.parse(xhr.responseText)); 
    }
}

// Hit the server
xhr.open("GET","/polygon/"+"A85007", true);
xhr.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
xhr.send();

var add_polygon = function(poly){
    var points = []
    var x = 0;
    var y = 0;
    for (var i = 0; i<poly.points.length; i++){
        points.push(L.latLng(poly.points[i][0], poly.points[i][1]));
        x += poly.points[i][0];
        y += poly.points[i][1];
    }
    x /= poly.points.length;
    y /= poly.points.length;
    var p = L.polygon(points);
    map.addLayer(p);
    map.setView([x, y]);
    map.setZoom(12);
}

