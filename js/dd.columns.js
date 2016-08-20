$(function() {
    theTable = $("#tablesorter-data").tablesorter({sortList:[[0,0],[2,0]], widgets: ['zebra']});
    $("#options").tablesorter({sortList: [[0,0]], headers: { 3:{sorter: false}, 4:{sorter: false}}});
    theTable.find("tbody > tr").find("td:eq(1)").mousedown(function(){
      $(this).prev().find(":checkbox").click()
    });
    $("#filter").keyup(function() {
      $.uiTableFilter( theTable, this.value );
    });
    $('#filter-form').submit(function(){
      theTable.find("tbody > tr:visible > td:eq(1)").mousedown();
      return false;
    }).focus(); //Give focus to input field
});
