package com.tribbloids.spookystuff.utils;

import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * from:
 * http://www.java2s.com/Code/Java/JDK-7/CopyingadirectoryusingtheSimpleFileVisitorclass.htm
 */
class CopyDirectoryFileVisitor extends SimpleFileVisitor<Path> {

  private Path source;
  private Path target;

  public CopyOption[] options = {
          StandardCopyOption.COPY_ATTRIBUTES,
          StandardCopyOption.REPLACE_EXISTING
//          StandardCopyOption.ATOMIC_MOVE
  };

  public CopyDirectoryFileVisitor(Path source, Path target) {
    this.source = source;
    this.target = target;
  }

  @Override
  public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
          throws IOException {
    Path dst = getTransitive(file);
    SpookyUtils.blockingCopy(file, dst, options);
    return FileVisitResult.CONTINUE;
  }

  private Path getTransitive(Path file) {
    Path result = target.resolve(source.relativize(file).toString());
    try {
      Files.createDirectories(result.getParent());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return result;
  }

  @Override
  public FileVisitResult preVisitDirectory(Path directory,
                                           BasicFileAttributes attributes) throws IOException {
    Path targetDirectory = getTransitive(directory);
    try {
      Files.copy(directory, targetDirectory, options); //TODO: change to SpookyUtils.blockingCopy
    }
    catch (FileAlreadyExistsException | DirectoryNotEmptyException e) {
      if (!Files.isDirectory(targetDirectory)) {
        throw e;
      }
    }
    return FileVisitResult.CONTINUE;
  }
}
