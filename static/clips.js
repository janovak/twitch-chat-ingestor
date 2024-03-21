document.addEventListener("DOMContentLoaded", function () {
    fetchVideos();
});

function fetchVideos() {
    let currentDate = new Date();
    const currentISODate = currentDate.toISOString();
    const twentyFourHoursAgoDate = new Date(currentDate.getTime() - (24 * 60 * 60 * 1000));
    const twentyFourHoursAgoISODate = twentyFourHoursAgoDate.toISOString();

    // Construct the API URL with start and stop timestamps
    const apiUrl = `https://www.streamer-summaries.com:443/v1.0/clip?start=${currentISODate}&end=${twentyFourHoursAgoISODate}`;
    fetch(apiUrl)
        .then(response => response.json())
        .then(data => {
            const thumbnailsContainer = document.getElementById('videoContainer');

            // Display thumbnails
            data.clips.forEach(thumbnailData => {
                const thumbnailUrl = thumbnailData.thumbnail_url;
                const embeddedUrl = thumbnailData.embed_url;

                // Create a new image element to get dimensions
                const img = new Image();
                img.onload = function () {
                    const thumbnailElement = document.createElement('img');
                    thumbnailElement.src = thumbnailUrl;
                    thumbnailElement.style.cursor = 'pointer'; // Add pointer cursor

                    // Set width and height based on image dimensions
                    thumbnailElement.style.width = img.naturalWidth + 'px';
                    thumbnailElement.style.height = img.naturalHeight + 'px';

                    thumbnailElement.addEventListener('click', () => {
                        // Load embedded URL when thumbnail is clicked
                        loadEmbeddedUrl(embeddedUrl, thumbnailElement);
                    });
                    thumbnailsContainer.appendChild(thumbnailElement);

                };
                img.src = thumbnailUrl; // Start loading the image
            });
        })
        .catch(error => console.error('Error fetching thumbnails:', error));
}

function loadEmbeddedUrl(embeddedUrl, thumbnailElement) {
    const iframe = document.createElement('iframe');
    iframe.src = embeddedUrl + "&parent=www.streamer-summaries.com" + "&autoplay=true";
    iframe.setAttribute('allow', 'autoplay; fullscreen');

    // Set iframe size to match thumbnail
    const thumbnailRect = thumbnailElement.getBoundingClientRect();
    iframe.style.width = thumbnailRect.width + 'px';
    iframe.style.height = thumbnailRect.height + 'px';

    // Replace the thumbnail with the iframe
    thumbnailElement.parentNode.replaceChild(iframe, thumbnailElement);
}

