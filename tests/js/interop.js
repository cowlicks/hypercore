const Hypercore = require('hypercore');

// Static test key pair obtained with:
//
//   const crypto = require('hypercore-crypto');
//   const keyPair = crypto.keyPair();
//   console.log("public key", keyPair.publicKey.toString('hex').match(/../g).join(' '));
//   console.log("secret key", keyPair.secretKey.toString('hex').match(/../g).join(' '));
const testKeyPair = {
    publicKey: Buffer.from([
        0x97, 0x60, 0x6c, 0xaa, 0xd2, 0xb0, 0x8c, 0x1d, 0x5f, 0xe1, 0x64, 0x2e, 0xee, 0xa5, 0x62, 0xcb,
        0x91, 0xd6, 0x55, 0xe2, 0x00, 0xc8, 0xd4, 0x3a, 0x32, 0x09, 0x1d, 0x06, 0x4a, 0x33, 0x1e, 0xe3]),
    secretKey: Buffer.from([
        0x27, 0xe6, 0x74, 0x25, 0xc1, 0xff, 0xd1, 0xd9, 0xee, 0x62, 0x5c, 0x96, 0x2b, 0x57, 0x13, 0xc3,
        0x51, 0x0b, 0x71, 0x14, 0x15, 0xf3, 0x31, 0xf6, 0xfa, 0x9e, 0xf2, 0xbf, 0x23, 0x5f, 0x2f, 0xfe,
        0x97, 0x60, 0x6c, 0xaa, 0xd2, 0xb0, 0x8c, 0x1d, 0x5f, 0xe1, 0x64, 0x2e, 0xee, 0xa5, 0x62, 0xcb,
        0x91, 0xd6, 0x55, 0xe2, 0x00, 0xc8, 0xd4, 0x3a, 0x32, 0x09, 0x1d, 0x06, 0x4a, 0x33, 0x1e, 0xe3]),
}

if (process.argv.length !== 4) {
    console.error("Usage: node interop.js [test step] [test set]")
    process.exit(1);
}

if (process.argv[2] === '1') {
    step1Create(process.argv[3]).then(result => {
        console.log("step1 ready", result);
    });
} else if (process.argv[2] === '2'){
    step2AppendHelloWorld(process.argv[3]).then(result => {
        console.log("step2 ready", result);
    });
} else if (process.argv[2] === '3'){
    step3iReadAndAppendUnflushed(process.argv[3]).then(result => {
        console.log("step3 ready", result);
    });
} else {
    console.error(`Invalid test step {}`, process.argv[2]);
    process.exit(2);
}

async function step1Create(testSet) {
    const core = new Hypercore(`work/${testSet}`, testKeyPair.publicKey, {keyPair: testKeyPair});
    await core.close();
};

async function step2AppendHelloWorld(testSet) {
    const core = new Hypercore(`work/${testSet}`, testKeyPair.publicKey, {keyPair: testKeyPair});
    const result = await core.append([Buffer.from('Hello'), Buffer.from('World')]);
    if (result.length != 2 || result.byteLength != 10) {
        throw new Error(`Invalid append result: ${result.length} or ${result.byteLength}`);
    }
    await core.close();
};

async function step3iReadAndAppendUnflushed(testSet) {
    const core = new Hypercore(`work/${testSet}`, testKeyPair.publicKey, {keyPair: testKeyPair});
    const hello = (await core.get(0)).toString();
    const world = (await core.get(1)).toString();
    if (hello != "Hello" || world != "World")  {
        throw new Error(`Read invalid data from hypercore: ${hello} or ${world}`);
    }
    let result = await core.append(Buffer.from('first'));
    if (result.length != 3 || result.byteLength != 15) {
        throw new Error(`Invalid append result: ${result.length} or ${result.byteLength}`);
    }
    result = await core.append([Buffer.from('second'), Buffer.from('third')]);
    if (result.length != 5 || result.byteLength != 26) {
        throw new Error(`Invalid append result: ${result.length} or ${result.byteLength}`);
    }
    const first = (await core.get(2)).toString();
    const second = (await core.get(3)).toString();
    const third = (await core.get(4)).toString();
    if (first != "first" || second != "second" || third != "third")  {
        throw new Error(`Read invalid data from hypercore: ${first} or ${second} or ${third}`);
    }
    await core.close();
};
