import { base64urlToJSON } from '@ceramicnetwork/common'

let inputData = '';

process.stdin.on('data', (data) => {
  inputData += data.toString();
});

process.stdin.on('end', () => {
  inputData = inputData.trim();
  const decodedData = base64urlToJSON(inputData);
  console.log(JSON.stringify(decodedData, null, 2));
});

process.stdin.resume();
