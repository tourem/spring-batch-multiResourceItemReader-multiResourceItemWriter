package com.larbotech.batch.reader;

import static java.nio.file.Files.newDirectoryStream;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;

import com.larbotech.batch.model.Employee;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.StreamSupport;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

public class CustomMultiResourceItemReader extends MultiResourceItemReader<Employee> {


  public CustomMultiResourceItemReader(String dirPath) throws IOException {

    FileSystemResource[] resources;

    try (DirectoryStream<Path> pathDirectoryStream = newDirectoryStream(Paths.get(dirPath),
        path -> path.toString().endsWith(".csv"))) {
      resources = StreamSupport
          .stream(spliteratorUnknownSize(pathDirectoryStream.iterator(), ORDERED), false)
          .map(FileSystemResource::new)
          .toArray(FileSystemResource[]::new);
    }
    setResources(resources);
  }

  @Override
  public void setResources(Resource[] resources) {
    super.setResources(resources);
  }
}
