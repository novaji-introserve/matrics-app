function hello() {
    console.log("hello, from olumide");

}
hello()
function loadCSS(filename) {
    const link = document.createElement("link");
    link.rel = "stylesheet";
    link.type = "text/css";
    link.href = filename;
    document.head.appendChild(link);
}

// Usage
loadCSS("rule_book/static/src/css/tree_view.css");
// loadCSS("rule_book/static/src/css/form_style.css");


// The above code is suppose to change the whole team of the whole application
