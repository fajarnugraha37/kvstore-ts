import { faker } from '@faker-js/faker';
import { Wal } from "./libs";

const wal = new Wal();
await wal.open();
const kv = new Map<string, string>();

console.log("Appending entries to WAL...");
for (let index = 0; index < 1000; index++) {
    await wal.append({ 
        key: `key${index}`, 
        v: `value${index}`, 
        ts: Date.now(),
        fullName: faker.person.fullName(),
        email: faker.internet.email(),
     });
    kv.set(`key${index}`, `value${index}`);
}

console.log("WAL entries written.");
for await (const entry of wal.scan()) {
    console.log(entry);
}