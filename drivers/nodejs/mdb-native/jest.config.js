/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/tests'],
  // `tests/test_native.ts` 是脚本式自测（自带 assert 与 main），不是 jest 用例，
  // 单独用 `npm run test:native` 跑。
  testMatch: ['**/*.test.ts'],
  moduleFileExtensions: ['ts', 'js', 'json'],
};
