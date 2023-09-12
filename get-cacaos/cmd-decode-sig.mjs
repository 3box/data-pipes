import { base64urlToJSON } from '@ceramicnetwork/common'

let inputData = '';

process.stdin.on('data', (data) => {
  inputData += data.toString();
});

process.stdin.on('end', () => {
  inputData = inputData.trim();
  const decodedData = base64urlToJSON(inputData);
  console.log(decodedData);
});

process.stdin.resume();
