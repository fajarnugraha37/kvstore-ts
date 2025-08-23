export type Constructor<T = {}> = new (...args: any[]) => T;

// Helper type to extract constructor parameters
export type ConstructorParameters<T> = T extends new (...args: infer P) => any ? P : never;

// Mixin function that preserves type safety
export function applyMixins<T extends Constructor, U extends Constructor[]>(
    Base: T,
    ...mixins: U
): T & Constructor<UnionToIntersection<InstanceType<U[number]>>> {
    class Mixed extends Base {}

    mixins.forEach((mixin) => {
        Object.getOwnPropertyNames(mixin.prototype).forEach((name) => {
            if (name !== 'constructor') {
                Object.defineProperty(
                    Mixed.prototype,
                    name,
                    Object.getOwnPropertyDescriptor(mixin.prototype, name) || Object.create(null)
                );
            }
        });
    });

    return Mixed as any;
}

// Utility type to convert union to intersection
type UnionToIntersection<U> = (U extends any ? (k: U) => void : never) extends (k: infer I) => void ? I : never;

// Mixin decorator for easier usage
function Mixin<T extends Constructor[]>(...mixins: T) {
    return function <U extends Constructor>(Base: U) {
        return applyMixins(Base, ...mixins);
    };
}

// Example usage:
class Flyable {
  fly() { console.log('Flying'); }
}

class Swimmable {
  swim() { console.log('Swimming'); }
}

@Mixin(Flyable, Swimmable)
class Duck {
  quack() { console.log('Quack'); }
}

const duck = new Duck();
// duck.fly();    // TypeScript knows this exists
// duck.swim();   // TypeScript knows this exists
// duck.quack();  // Original method still available