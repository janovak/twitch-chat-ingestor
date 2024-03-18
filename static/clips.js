document.addEventListener("DOMContentLoaded", function () {
    fetchVideos();
});

function fetchVideos() {
    let currentDate = new Date();
    const currentISODate = currentDate.toISOString();
    const twentyFourHoursAgoDate = new Date(currentDate.getTime() - (7 * 24 * 60 * 60 * 1000));
    const twentyFourHoursAgoISODate = twentyFourHoursAgoDate.toISOString();

    // Construct the API URL with start and stop timestamps
    const apiUrl = `https://streamer-summaries.com:443/v1.0/clip?start=${currentISODate}&end=${twentyFourHoursAgoISODate}`;

    fetch(apiUrl)
        .then(response => response.json())
        .then(data => renderVideos(data.clip_urls))
        .catch(error => console.error("Error fetching videos:", error));

}

function renderVideos(videoLinks) {
    const videoContainer = document.getElementById("videoContainer");

    videoLinks.forEach(link => {
        // Append &parent=janovak.github.io to each link
        const modifiedLink = link + "&parent=streamer-summaries.com";

        // Create iframe element
        const iframeElement = document.createElement("iframe");
        iframeElement.src = modifiedLink;
        iframeElement.frameborder = "0";
        iframeElement.allowfullscreen = true;

        // Append iframe element to container
        videoContainer.appendChild(iframeElement);
    });
}

