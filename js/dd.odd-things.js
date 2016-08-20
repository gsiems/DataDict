$(function() {
    tabTable = $("#tablesorter-tables").tablesorter({sortList:[[0,0]], widgets: ['zebra']});
    colTable = $("#tablesorter-columns").tablesorter({sortList:[[0,0]], widgets: ['zebra']});
    $("#options").tablesorter({sortList: [[0,0]], headers: { 3:{sorter: false}, 4:{sorter: false}}});

    tabTable.find("tbody > tr").find("td:eq(1)").mousedown(function(){
      $(this).prev().find(":checkbox").click()
    });
    colTable.find("tbody > tr").find("td:eq(1)").mousedown(function(){
      $(this).prev().find(":checkbox").click()
    });
    $("#filter").keyup(function() {
      $.uiTableFilter( tabTable, this.value );
      $.uiTableFilter( colTable, this.value );
    });
    $('#filter-form').submit(function(){
      tabTable.find("tbody > tr:visible > td:eq(1)").mousedown();
      colTable.find("tbody > tr:visible > td:eq(1)").mousedown();
      return false;
    }).focus(); //Give focus to input field

});
