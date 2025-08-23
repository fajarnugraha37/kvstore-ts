export type Constructor<T = {}> = new (...args: any[]) => T;

// Helper type to extract constructor parameters
export type ConstructorParameters<T> = T extends new (...args: infer P) => any
  ? P
  : never;

// Utility type to convert union to intersection
export type UnionToIntersection<U> = (
  U extends any ? (k: U) => void : never
) extends (k: infer I) => void
  ? I
  : never;

// Mixin function that preserves type safety
// Example usage
// class CanEat {
//   eat() { console.log("eating"); }
// }

// class CanWalk {
//   walk() { console.log("walking"); }
// }

// class Animal {
//   name: string;
//   constructor(name: string) { this.name = name; }
// }

// // Create a new class that extends Animal and gets methods from CanEat and CanWalk
// const MixedAnimal = applyMixins(Animal, CanEat, CanWalk);

// const a = new MixedAnimal("bunny");
// a.eat();   // eating
// a.walk();  // walking
// console.log(a.name); // "bunny"
export function applyMixins<
  TBase extends Constructor,
  TMixins extends Constructor[]
>(
  Base: TBase,
  ...mixins: TMixins
): new (...args: ConstructorParameters<TBase>) => InstanceType<TBase> &
  UnionToIntersection<InstanceType<TMixins[number]>> {
  class Mixed extends Base {
    constructor(...args: any[]) {
      super(...args);
    }
  }

  mixins.forEach((mixin) => {
    Object.getOwnPropertyNames(mixin.prototype).forEach((name) => {
      if (name !== "constructor") {
        Object.defineProperty(
          Mixed.prototype,
          name,
          Object.getOwnPropertyDescriptor(mixin.prototype, name) ||
            Object.create(null)
        );
      }
    });
  });

  return Mixed as any;
}

// Export the Mixin decorator for external use
// class CanEat {
//   eat() { console.log("eating"); }
// }

// class CanWalk {
//   walk() { console.log("walking"); }
// }

// class Animal {
//   name: string;
//   constructor(name: string) { this.name = name; }
// }

// @Mixin(CanEat, CanWalk)
// class Pet extends Animal {
//   // Pet still has Animal constructor and its own members
//   play() { console.log("playing"); }
// }

// const p = new Pet("fido");
// p.eat();
// p.walk();
// p.play();
export function Mixin<TMixins extends Constructor[]>(...mixins: TMixins) {
  return function <TBase extends Constructor>(
    Base: TBase
  ): new (...args: ConstructorParameters<TBase>) => InstanceType<TBase> &
    UnionToIntersection<InstanceType<TMixins[number]>> {
    return applyMixins(Base, ...mixins);
  };
}
