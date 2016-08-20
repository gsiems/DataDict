$(function() {
    fkTable = $("#tablesorter-fkdata").tablesorter({sortList:[[1,0],[0,0],[3,0]], widgets: ['zebra']});
    chkTable = $("#tablesorter-chkdata").tablesorter({sortList:[[0,0],[2,0]], widgets: ['zebra']});
    uTable = $("#tablesorter-udata").tablesorter({sortList:[[0,0],[2,0]], widgets: ['zebra']});
    $("#options").tablesorter({sortList: [[0,0]], headers: { 3:{sorter: false}, 4:{sorter: false}}});

    fkTable.find("tbody > tr").find("td:eq(1)").mousedown(function(){
      $(this).prev().find(":checkbox").click()
    });
    chkTable.find("tbody > tr").find("td:eq(1)").mousedown(function(){
      $(this).prev().find(":checkbox").click()
    });
    uTable.find("tbody > tr").find("td:eq(1)").mousedown(function(){
      $(this).prev().find(":checkbox").click()
    });
    $("#filter").keyup(function() {
      $.uiTableFilter( fkTable, this.value );
      $.uiTableFilter( chkTable, this.value );
      $.uiTableFilter( uTable, this.value );
    });
    $('#filter-form').submit(function(){
      fkTable.find("tbody > tr:visible > td:eq(1)").mousedown();
      chkTable.find("tbody > tr:visible > td:eq(1)").mousedown();
      uTable.find("tbody > tr:visible > td:eq(1)").mousedown();
      return false;
    }).focus(); //Give focus to input field

});
