/*
  Creates enums with assignable string literals
  https://github.com/microsoft/TypeScript/issues/3192#issuecomment-261720275
*/
export default function makeEnum<T extends { [index: string]: U }, U extends string>(x: T) {
  return x;
}
