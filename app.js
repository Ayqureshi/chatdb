document.getElementById('send-button').addEventListener('click', sendMessage);

function sendMessage() {
    const userInput = document.getElementById('user-input');
    const message = userInput.value.trim();

    if (message) {
        // Add the user message to the chat
        addMessageToChat(message, 'user');
        // Add the query to the list for tracking
        addQueryToList(message);
        userInput.value = '';  // Clear the input field
    }
}

// Function to add a message to the chat
function addMessageToChat(message, sender) {
    const chatBox = document.getElementById('chat-box');
    const messageElement = document.createElement('div');
    messageElement.classList.add('chat-message', sender);
    messageElement.textContent = message;
    chatBox.appendChild(messageElement);
    chatBox.scrollTop = chatBox.scrollHeight; // Scroll to the bottom
}

// Function to add a query to a list for tracking
function addQueryToList(query) {
    const queryList = document.getElementById('query-list');
    if (!queryList) {
        console.error('Query list element not found');
        return;
    }
    const queryItem = document.createElement('li');
    queryItem.classList.add('query-item');
    queryItem.textContent = query;
    queryList.appendChild(queryItem);
}

function uploadFile() {
    var fileInput = document.getElementById('fileInput');
    var resultsDiv = document.getElementById('results');
    
    if (fileInput.files.length > 0) {
        var file = fileInput.files[0];
        var formData = new FormData();
        formData.append('file', file);

        fetch('http://localhost:5000/api/upload', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            resultsDiv.innerHTML = `<pre>${JSON.stringify(data, null, 2)}</pre>`;
        })
        .catch(error => {
            console.error('Error:', error);
            resultsDiv.innerHTML = `<p>Error uploading file. Please check the console.</p>`;
        });
    } else {
        resultsDiv.innerHTML = `<p>Please select a file to upload.</p>`;
    }
}
