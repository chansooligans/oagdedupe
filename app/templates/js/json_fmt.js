var sample1 = {{ sample1|tojson }}
document.getElementById("sample1").textContent = JSON.stringify(sample1, undefined, 2);

var sample2 = {{ sample2|tojson }}
document.getElementById("sample2").textContent = JSON.stringify(sample2, undefined, 2);

var labels = {{ labels|tojson }}
document.getElementById("labels").textContent = JSON.stringify(labels, undefined, 2);