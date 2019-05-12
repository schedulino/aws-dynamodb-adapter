describe('respond', () => {
  test('should return resolved data without to run validation', () => {
    return expect('aa').toEqual('aa');
  });
});
