$('#dataset-menu li').click(function(){
    $('#dropdownMenuButton1').html($(this).text() + '<span class="caret"></span>')
    $('#dataset-hidden-selection').attr('value', $(this).text());
})
