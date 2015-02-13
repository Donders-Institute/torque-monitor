/** parse URL to get the URL parameters **/
function getUrlVar(key){
    var result = new RegExp(key + "=([^&]*)", "i").exec(window.location.search); 
    return result && unescape(result[1]) || ""; 
}

/** generate an array of integer **/
function range(start, end) {
    var foo = [];
    for (var i = start; i <= end; i++) {
        foo.push(i);
    }
    return foo;
}
