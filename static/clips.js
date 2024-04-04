document.addEventListener("DOMContentLoaded", function () {
    fetchVideos();
});

function fetchVideos() {
    let currentDate = new Date();
    const currentISODate = currentDate.toISOString();
    const oneWeekAgoDate = new Date(currentDate.getTime() - (7 * 24 * 60 * 60 * 1000));
    const oneWeekAgoISODate = oneWeekAgoDate.toISOString();

    // Construct the API URL with start and stop timestamps
    const apiUrl = `https://www.streamer-summaries.com:443/v1.0/clip?start=${currentISODate}&end=${oneWeekAgoISODate}`;
    fetch(apiUrl)
        .then(response => response.json())
        .then(data => {
            const thumbnailsContainer = document.getElementById('videoContainer');
            const thumbnailPromises = [];

            data.clips.forEach(thumbnailData => {
                const thumbnailUrl = thumbnailData.thumbnail_url;
                const embeddedUrl = thumbnailData.embed_url;
                const timestamp = thumbnailData.timestamp;

                const promise = new Promise((resolve, reject) => {
                    const img = new Image();
                    img.onload = function () {
                        const thumbnailElement = document.createElement('img');
                        thumbnailElement.src = thumbnailUrl;
                        thumbnailElement.style.cursor = 'pointer';
                        thumbnailElement.style.width = img.naturalWidth + 'px';
                        thumbnailElement.style.height = img.naturalHeight + 'px';

                        thumbnailElement.addEventListener('click', () => {
                            loadEmbeddedUrl(embeddedUrl, thumbnailElement);
                        });

                        resolve({ element: thumbnailElement, timestamp: timestamp });
                    };
                    img.onerror = reject;
                    img.src = thumbnailUrl;
                });

                thumbnailPromises.push(promise);
            });

            Promise.all(thumbnailPromises)
                .then(thumbnails => {
                    thumbnails.sort((a, b) => b.timestamp - a.timestamp);
                    thumbnails.forEach(thumbnail => {
                        thumbnailsContainer.appendChild(thumbnail.element);
                    });
                })
                .catch(error => console.error('Error loading thumbnails:', error));
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
