package com.tribbloids.spookystuff.uav.spatial.map

import breeze.linalg.DenseVector
import com.tribbloids.spookystuff.uav.spatial.Spatial
import com.tribbloids.spookystuff.uav.spatial.point.NED
import com.tribbloids.spookystuff.uav.spatial.util.UnknownTrellisGeometry

trait VoxelSketch extends Spatial {}

/**
  * Ideally this sketch should have both properties of bloom filters and E2LSH:
  * each occupied voxel is registered in several different bloom filters.
  * after all voxels are registered, for any new point, its distance to the closest occupied voxel
  * can be approximated by the number/ratio of bloom filters that has returns positives.
  */
case class VoxelSketchImpl() extends VoxelSketch {
  override def system = NED

  override def vector: DenseVector[Double] = ???
}
