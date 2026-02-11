'use strict';

const o = Object.freeze({a: Object.freeze([1])});

// o.a.push(2);
o.a = 2;
