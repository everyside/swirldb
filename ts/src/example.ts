import { SwirlDB } from '@swirldb/core-wasm';

const db = new SwirlDB();

db.observe('user.name', (newVal: any) => {
  console.log('user.name changed:', newVal);
});


// Set values using path-based keys
db.setPath('user.name', 'Dani');
db.setPath('user.age', 47);

console.log('user.name:', db.getPath('user.name'));
console.log('user.age:', db.getPath('user.age'));

// Save state
const state = db.saveState();

// Create a new DB and restore the saved state
const db2 = new SwirlDB();
db2.loadState(state);

console.log('Restored user.name:', db2.getPath('user.name'));

