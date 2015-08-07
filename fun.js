
import {curry} from 'ramda'

export class Maybe {

  constructor (x) {
    this.__value = x
  }

  static of (x) {
    return new Maybe(x)
  }

  isNothing () {
    return (this.__value === null || this.__value === undefined)
  }

  map (f) {
    return this.isNothing() ? Maybe.of(null) : Maybe.of(f(this.__value))
  }

}

export var map = curry(function(f, any_functor_at_all) {
  return any_functor_at_all.map(f)
})
