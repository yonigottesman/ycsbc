/*
 * flurry_generator.h
 *
 *  Created on: Apr 3, 2017
 *      Author: erangi
 */

#ifndef CORE_FLURRY_GENERATOR_H_
#define CORE_FLURRY_GENERATOR_H_

#include "generator.h"

#include <cstdint>
#include <cmath>
#include "zipfian_generator.h"
#include "utils.h"

namespace ycsbc
{

class FlurryGenerator : public Generator<uint64_t> {
  void setPartSizes(uint64_t num_items, size_t paddingBits) {
      size_t bits = (size_t)(std::log2(num_items) + 0.5);
      size_t halfPaddingBits = paddingBits / 2;
      size_t p1Bits = bits / 2 - halfPaddingBits;
      size_t p2Bits = bits - p1Bits - paddingBits;
      part1Shift = p2Bits + paddingBits;
      part1NumItems = (1 << p1Bits) - 1;
      part2NumItems = (1 << p2Bits) - 1;
      // limit MSBs to those of the full number of items
      part1NumItems = std::min(num_items >> p2Bits, part1NumItems);
  }
 public:
  FlurryGenerator(uint64_t num_items, size_t paddingChars,
      double zipfian_const = ZipfianGenerator::kZipfianConst) :
      num_items_([=]{ setPartSizes(num_items, paddingChars); return num_items;}()),
      generator1(0, part1NumItems, zipfian_const),
      generator2(0, part2NumItems, zipfian_const), last_(0) {
//        setPartSizes(num_items, paddingChars);
//        generator1.Next(part1NumItems);
//        generator2.Next(part2NumItems);
    }

  uint64_t Next();
  uint64_t Last() { return last_; }

 private:
  uint64_t num_items_;
  ZipfianGenerator generator1;
  ZipfianGenerator generator2;
  uint64_t last_;
  uint64_t part1NumItems;
  uint64_t part2NumItems;
  uint64_t part1Shift;
};

inline uint64_t FlurryGenerator::Next() {
//  uint64_t value1 = utils::FNVHash64(generator1.Next()) % part1NumItems;
  uint64_t value1 = generator1.Next() % part1NumItems;
  uint64_t value2 = utils::FNVHash64(generator2.Next()) % part2NumItems;
  return last_ = (value1 << part1Shift) | value2;
}

} /* namespace ycsbc */



#endif /* CORE_FLURRY_GENERATOR_H_ */
