#!/usr/bin/env node
import puppeteer from 'puppeteer';
import {fileURLToPath} from 'url';
import path from 'path';
import fs from 'fs/promises';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const outputFile = path.join(
  __dirname,
  '..',
  '..',
  'demo/frontend/puzzle-pieces.ts',
);

const PIECE_LETTERS = [
  'A',
  'A',
  'A',
  'A',
  'A',
  'A',
  'A',
  'L',
  'L',
  'L',
  'I',
  'I',
  'I',
  'I',
  'V',
  'V',
  'V',
  'V',
  'V',
  'V',
  'V',
  'E',
  'E',
  'E',
  'E',
  'E',
  'E',
  'E',
];

(async () => {
  const browser = await puppeteer.launch();
  const page = await browser.newPage();
  await page.goto(`file://${path.join(__dirname, 'alive-pieces.svg')}`);
  const positions = await page.evaluate(() => {
    const pieces = Array.from(document.querySelectorAll('svg > path'));
    return pieces.map(piece => {
      const {x, y} = piece.getBBox();
      return {
        dx: x,
        dy: y,
      };
    });
  });
  let output = [
    "import {SVG_ORIGINAL_SIZE} from '../shared/constants';",
    "import {Letter, PuzzlePiece} from '../shared/types';",
    '',
  ];
  let fileIdx = 1;
  let lastLetter = '';
  // Paths are reversed in the file
  positions.reverse();
  for await (const [index, pos] of positions.entries()) {
    const letter = PIECE_LETTERS[index];
    if (lastLetter !== letter) {
      if (lastLetter !== '') {
        output.push(`];\n`);
      }
      fileIdx = 1;
      lastLetter = letter;
      output.push(`const ${letter}: PuzzlePiece[] = [`);
    } else {
      fileIdx++;
    }
    const filename = letter.toLowerCase() + fileIdx;
    const file = filename + '.svg';
    await page.goto(`file://${path.join(__dirname, file)}`);
    const pieceInfo = await page.evaluate(() => {
      const svg = document.querySelector('svg');
      return {
        width: svg.getAttribute('width'),
        height: svg.getAttribute('height'),
        path: document.querySelector('path').getAttribute('d'),
      };
    });
    output.push(`// ${filename}
    {
      letter: Letter.${letter},
      paths: ['${pieceInfo.path}'],
      width: ${pieceInfo.width},
      height: ${pieceInfo.height},
      dx: ${pos.dx} / SVG_ORIGINAL_SIZE.width,
      dy: ${pos.dy} / SVG_ORIGINAL_SIZE.height,
    },`);
  }
  output.push(`];\n`);
  output.push('export const PUZZLE_PIECES = [...A,...L,...I,...V,...E];');
  browser.close();
  await fs.writeFile(outputFile, output.join('\n'), 'utf-8');
})();
