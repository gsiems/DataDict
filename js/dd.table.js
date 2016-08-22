$(function() {
    colTable = $("#tablesorter-coldata").tablesorter({sortList:[[1,0]], widgets: ['zebra']});
    consTable = $("#tablesorter-consdata").tablesorter({sortList:[[1,0],[0,0]], widgets: ['zebra']});
    idxTable = $("#tablesorter-idxdata").tablesorter({sortList:[[0,0]], widgets: ['zebra']});
    ikeyTable = $("#tablesorter-ikeydata").tablesorter({sortList:[[0,0]], widgets: ['zebra']});
    ekeyTable = $("#tablesorter-ekeydata").tablesorter({sortList:[[0,0]], widgets: ['zebra']});
    pdepTable = $("#tablesorter-pdepdata").tablesorter({sortList:[[0,0]], widgets: ['zebra']});
    cdepTable = $("#tablesorter-cdepdata").tablesorter({sortList:[[0,0]], widgets: ['zebra']});
    wrapTable = $("#tablesorter-wrapperdata").tablesorter({sortList:[[0,0]], widgets: ['zebra']});

    $("#options").tablesorter({sortList: [[0,0]], headers: { 3:{sorter: false}, 4:{sorter: false}}});

    colTable.find("tbody > tr").find("td:eq(1)").mousedown(function(){
      $(this).prev().find(":checkbox").click()
    });
    consTable.find("tbody > tr").find("td:eq(1)").mousedown(function(){
      $(this).prev().find(":checkbox").click()
    });
    idxTable.find("tbody > tr").find("td:eq(1)").mousedown(function(){
      $(this).prev().find(":checkbox").click()
    });
    ikeyTable.find("tbody > tr").find("td:eq(1)").mousedown(function(){
      $(this).prev().find(":checkbox").click()
    });
    ekeyTable.find("tbody > tr").find("td:eq(1)").mousedown(function(){
      $(this).prev().find(":checkbox").click()
    });
    pdepTable.find("tbody > tr").find("td:eq(1)").mousedown(function(){
      $(this).prev().find(":checkbox").click()
    });
    cdepTable.find("tbody > tr").find("td:eq(1)").mousedown(function(){
      $(this).prev().find(":checkbox").click()
    });
    wrapTable.find("tbody > tr").find("td:eq(1)").mousedown(function(){
      $(this).prev().find(":checkbox").click()
    });
      $("#filter").keyup(function() {
      $.uiTableFilter( colTable, this.value );
      $.uiTableFilter( consTable, this.value );
      $.uiTableFilter( idxTable, this.value );
      $.uiTableFilter( ikeyTable, this.value );
      $.uiTableFilter( ekeyTable, this.value );
      $.uiTableFilter( pdepTable, this.value );
      $.uiTableFilter( cdepTable, this.value );
      $.uiTableFilter( wrapTable, this.value );
    });
    $('#filter-form').submit(function(){
      colTable.find("tbody > tr:visible > td:eq(1)").mousedown();
      consTable.find("tbody > tr:visible > td:eq(1)").mousedown();
      idxTable.find("tbody > tr:visible > td:eq(1)").mousedown();
      ikeyTable.find("tbody > tr:visible > td:eq(1)").mousedown();
      ekeyTable.find("tbody > tr:visible > td:eq(1)").mousedown();
      pdepTable.find("tbody > tr:visible > td:eq(1)").mousedown();
      cdepTable.find("tbody > tr:visible > td:eq(1)").mousedown();
      wrapTable.find("tbody > tr:visible > td:eq(1)").mousedown();
      return false;
    }).focus(); //Give focus to input field

});
