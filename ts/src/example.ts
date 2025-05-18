import { SwirlDB } from '@swirldb/core-wasm';

const db = new SwirlDB();
db.set('foo', 'bar');
console.log('get(foo):', db.get('foo'));
