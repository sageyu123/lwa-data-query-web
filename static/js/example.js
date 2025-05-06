// static/js/example.js

document.addEventListener('DOMContentLoaded', function () {
    const baseUrl = isOvsa ? '/lwadata-query' : '';

    const startInput = document.getElementById('start');
    const endInput = document.getElementById('end');
    let movieOffsetDays = 0;

    const now = new Date();
    const past = new Date();
    past.setDate(now.getDate() - 7);

    const formatDate = (date) => date.toISOString().slice(0, 19);

    startInput.value = formatDate(past);
    endInput.value = formatDate(now);

    flatpickr(".datetime-picker", {
        enableTime: true,
        dateFormat: "Y-m-d\\TH:i:S",
        allowInput: true,
        time_24hr: true,
    });

    function updateFileList(elementId, files) {
        const listElement = document.getElementById(elementId);
        listElement.innerHTML = '';
        files.forEach(file => {
            const li = document.createElement('li');
            li.textContent = file;
            listElement.appendChild(li);
        });
    }

    function downloadAsTxt(dataArray, filename) {
        const textContent = dataArray.join('\n');
        const blob = new Blob([textContent], { type: 'text/plain' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }

    function updateSpecAndMovie(baseStart, offsetDays) {
        // const startDate = new Date(baseStart);
        // startDate.setDate(startDate.getDate() + offsetDays);
        // const shifted = startDate.toISOString().slice(0, 10);
        // const shiftedStart = `${shifted}T12:00:00`;
        const dateStr = baseStart.slice(0, 10);
        const baseDate = new Date(`${dateStr}T00:00:00`);
        baseDate.setUTCDate(baseDate.getUTCDate() + offsetDays);
        const newDateStr = baseDate.toISOString().slice(0, 10);
        const shiftedStart = `${newDateStr}T12:00:00`;

        const formData = new FormData();
        formData.append("start", shiftedStart);

        fetch(`${baseUrl}/api/flare/spec_movie`, {
            method: 'POST',
            body: formData
        })
        .then(res => res.json())
        .then(data => {
            const movieContainer = document.getElementById("movie-container");
            const moviePlayer = document.getElementById("movie-player");
            const movieSource = document.getElementById("movie-player-source");
            const movieMessage = document.getElementById("movie-message");
            const specImage = document.getElementById("spec-preview");
            const specMessage = document.getElementById("spec-message");

            let hasMovie = false;
            let hasSpec = false;

            if (data.movie_path) {
                movieSource.src = data.movie_path;
                moviePlayer.load();
                moviePlayer.style.display = "block";
                movieMessage.style.display = "none";
                hasMovie = true;
            } else {
                moviePlayer.style.display = "none";
                movieMessage.style.display = "block";
            }

            if (data.spec_png_path) {
                specImage.src = data.spec_png_path;
                specImage.style.display = "block";
                specMessage.style.display = "none";
                hasSpec = true;
            } else {
                specImage.style.display = "none";
                specMessage.style.display = "block";
            }

            movieContainer.style.display = (hasMovie || hasSpec) ? "block" : "none";
        });
    }

    function queryAndUpdate() {
        const start = startInput.value;
        const end = endInput.value;

        const formData = new FormData();
        formData.append('start', start);
        formData.append('end', end);

        fetch(`${baseUrl}/api/flare/query`, {
            method: 'POST',
            body: formData
        })
        .then(res => res.json())
        .then(data => {
            updateFileList('spec-list', data.spec_fits);
            updateFileList('image-lev1-list', data.slow_lev1);
            updateFileList('image-lev15-list', data.slow_lev15);

            document.getElementById('download-spec').onclick = () => downloadAsTxt(data.spec_fits, 'ovro-lwa_solar_spec_fits.txt');
            document.getElementById('download-image-lev1').onclick = () => downloadAsTxt(data.slow_lev1, 'ovro-lwa_solar_image_lev1_hdf.txt');
            document.getElementById('download-image-lev15').onclick = () => downloadAsTxt(data.slow_lev15, 'ovro-lwa_solar_image_lev15_hdf.txt');

            const baseStartDate = start;
            movieOffsetDays = 0;
            updateSpecAndMovie(baseStartDate, movieOffsetDays);

            fetch(`${baseUrl}/plot`, {
                method: 'POST',
                body: formData
            })
            .then(res => res.json())
            .then(result => {
                const plotJSON = JSON.parse(result.plot);
                Plotly.newPlot('plot-container', plotJSON.data, plotJSON.layout);
            });
        });
    }

    document.getElementById('query-btn').addEventListener('click', function (e) {
        e.preventDefault();
        queryAndUpdate();
    });

    document.getElementById('plus1day').addEventListener('click', () => {
        movieOffsetDays += 1;
        const baseStart = document.getElementById('start').value;
        updateSpecAndMovie(baseStart, movieOffsetDays);
    });

    document.getElementById('minus1day').addEventListener('click', () => {
        movieOffsetDays -= 1;
        const baseStart = document.getElementById('start').value;
        updateSpecAndMovie(baseStart, movieOffsetDays);
    });

    queryAndUpdate();
});
