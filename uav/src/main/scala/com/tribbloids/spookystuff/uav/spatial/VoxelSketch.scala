package com.tribbloids.spookystuff.uav.spatial

import breeze.util.BloomFilter

case class Octree(

                 )

trait VoxelSketch extends SpatialData {

}

/**
  * Ideally this sketch should have both properties of bloom filters and E2LSH:
  * each occupied voxel is registered in several different bloom filters.
  * after all voxels are registered, for any new point, its distance to the closest occupied voxel
  * can be approximated by the number/ratio of bloom filters that has returns positives.
  */
case class VoxelSketchImpl(
                          )