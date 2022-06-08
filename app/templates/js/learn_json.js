var sample1 = {{ sample1|tojson }}
var sample2 = {{ sample2|tojson }}

var meta = {{ meta|tojson }}
document.getElementById("meta").textContent = JSON.stringify(meta, undefined, 2);

$("#retrain").click(function() { 
    $.get('/retrain');
    $('.alert').show()
});