        function Rank(evt, pagrams) {
            // 处理选项卡的激活状态
            var i, tablinks;
            tablinks = document.getElementsByClassName("tablinks");
            for (i = 0; i < tablinks.length; i++) {
                tablinks[i].className = tablinks[i].className.replace(" active", "");
            }
            evt.currentTarget.className += " active";

            // 清除原图表
            var chart = document.querySelector(".map .chart");
            chart.innerHTML = '';  // 彻底清空所有子元素

            // 根据pagrams绘制不同的元素
            if (pagrams === 'fansCount') {
                // 创建输入框
                var inputTextArea = document.createElement("textarea");
                inputTextArea.id = "input_text";
                inputTextArea.rows = 4;
                inputTextArea.cols = 50;
                chart.appendChild(inputTextArea);

                // 创建按钮
                var sbutton = document.createElement("button");


                sbutton.textContent = "提交文本";
                sbutton.style.backgroundColor = "#6842be"; // 背景颜色
                sbutton.style.color = "#dddb67"; // 字体颜色
                sbutton.style.border = "none"; // 无边框
                sbutton.style.padding = "10px 20px"; // 内边距
                sbutton.style.cursor = "pointer"; // 鼠标指针样式
                sbutton.style.fontSize = "14px"; // 字体大小
                sbutton.style.borderRadius = "5px"; // 圆角半径
                sbutton.onclick = function () {
                    var inputText = document.getElementById("input_text").value; // 获取用户输入的文本
                    // 调用处理函数并获取处理结果
                    processText(inputText, pagrams, function (processedText) {
                        var dialog = document.createElement("div");
                        dialog.style.border = "3px solid #6842be";
                        dialog.style.padding = "25px";
                        dialog.style.color = "#dddb67";
                        dialog.style.marginTop = "2px";
                        dialog.style.marginBottom = "2px";
                        dialog.textContent = processedText;  // 显示处理结果
                        chart.appendChild(dialog);
                    });
                };
                chart.appendChild(sbutton);  // 将按钮添加到 chart 容器中

            } else if (pagrams === 'totalDuration') {
                var dialog = document.createElement("div");
                dialog.className = "chat-container";

                dialog.innerHTML = `
                <div class="messages" id="messages-container">
                    <!-- Message will appear here -->
                </div>
                <div class="input-container">
                    <input type="text" class="message-input" id="message-input" placeholder="请输入您想要查询的问题...">
                    <button class="send-button" onclick="sendMessage()">发送</button>
                </div>
            `;

                chart.appendChild(dialog);
            }
        }


function sendMessage() {
    var inputElement = document.getElementById("message-input");
    var message = inputElement.value;

    // Display user message
    displayMessage("我", message);

    // Clear input field
    inputElement.value = "";

    // Send message to server
    var xhr = new XMLHttpRequest();
    xhr.open("POST", "/", true);
    xhr.setRequestHeader("Content-Type", "application/json");

    xhr.onreadystatechange = function () {
        if (xhr.readyState === 4 && xhr.status === 200) {
            // Process response from server
            var response = JSON.parse(xhr.responseText);
            displayMessage(response.sender, response.message);
        }
    };

    var data = JSON.stringify({"message": message});
    xhr.send(data);
}

function displayMessage(sender, message) {
    var messagesContainer = document.getElementById("messages-container");
    var messageElement = document.createElement("div");
    messageElement.textContent = sender + ": " + message;
    messagesContainer.appendChild(messageElement);
    messagesContainer.scrollTop = messagesContainer.scrollHeight;
}

function processText(inputText, pagrams, callback) {
    $.ajax({
        type: 'POST',
        url: '/process_text',
        data: {input_text: inputText},
        success: function (response) {
            var processedText = JSON.parse(response.result);
            var formattedText = formatRecommendations(processedText);
            callback(formattedText);  // 使用回调函数将处理结果传回
        },
        error: function (xhr, status, error) {
            console.error('错误：', error);
        }
    });
}

function formatRecommendations(recommendations) {
    var formattedText = '';
    for (var i = 1; i < recommendations.length; i++) {
        if(recommendations[i].title !== ""){
            formattedText += '预测标题: ' + recommendations[i].title + '\n';
        }
        formattedText += '预测内容: ' + recommendations[i].content + '\n\n';
    }
    return formattedText;
}