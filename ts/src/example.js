"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var core_wasm_1 = require("@swirldb/core-wasm");
var db = new core_wasm_1.SwirlDB();
db.set('foo', 'bar');
console.log('get(foo):', db.get('foo'));
