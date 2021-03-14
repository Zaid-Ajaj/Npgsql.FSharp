var ghPages = require("gh-pages");

var packageUrl = "https://github.com/Zaid-Ajaj/Npgsql.FSharp.git";

console.log("Publishing to ", packageUrl);

ghPages.publish("app", {
    repo: packageUrl,
    dotfiles: true
}, function (e) {
    if (e === undefined) {
        console.log("Finished publishing succesfully");
    } else {
        console.log("Error occured while publishing :(", e);
    }
});