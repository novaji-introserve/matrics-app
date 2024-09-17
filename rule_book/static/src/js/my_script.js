function hello() {
    console.log("hello, from uche");
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

// The above code is suppose to change the whole team of the whole application
