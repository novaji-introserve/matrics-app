const urlParams = new URLSearchParams(window.location.hash.substring(1)); // Remove '#'

if (urlParams.has("model")) {
  const model = urlParams.get("model");
  if(model === 'res.users'){
    const elements = document.querySelectorAll(".dropdown-item"); //selects all elements that have the class dropdown-item
    elements.forEach((element) => {
      if (element.textContent.includes("Archive")) {
        // Apply styling or perform actions on the element
        element.style.display = "none";
      }
    });


  }
}


