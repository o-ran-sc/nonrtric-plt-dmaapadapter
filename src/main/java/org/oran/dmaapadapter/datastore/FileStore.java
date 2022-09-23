/*-
 * ========================LICENSE_START=================================
 * O-RAN-SC
 * %%
 * Copyright (C) 2021 Nordix Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ========================LICENSE_END===================================
 */

package org.oran.dmaapadapter.datastore;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.exceptions.ServiceException;
import org.springframework.http.HttpStatus;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class FileStore implements DataStore {

    ApplicationConfig applicationConfig;

    public FileStore(ApplicationConfig applicationConfig) {
        this.applicationConfig = applicationConfig;
    }

    @Override
    public Flux<String> listFiles(Bucket bucket, String prefix) {
        Path root = Path.of(applicationConfig.getPmFilesPath(), prefix);
        if (!root.toFile().exists()) {
            root = root.getParent();
        }

        List<String> result = new ArrayList<>();
        try (Stream<Path> stream = Files.walk(root, Integer.MAX_VALUE)) {

            stream.forEach(path -> filterListFiles(path, prefix, result));

            return Flux.fromIterable(result);
        } catch (Exception e) {
            return Flux.error(e);
        }
    }

    private void filterListFiles(Path path, String prefix, List<String> result) {
        if (path.toFile().isFile() && externalName(path).startsWith(prefix)) {
            result.add(externalName(path));
        }
    }

    private String externalName(Path f) {
        String fullName = f.toString();
        return fullName.substring(applicationConfig.getPmFilesPath().length());
    }

    public Mono<String> readFile(String bucket, String fileName) {
        return Mono.error(new ServiceException("readFile from bucket Not implemented", HttpStatus.CONFLICT));
    }

    @Override
    public Mono<String> readFile(Bucket bucket, String fileName) {
        try {
            String contents = Files.readString(path(fileName));
            return Mono.just(contents);
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    @Override
    public Mono<Boolean> createLock(String name) {
        File file = path(name).toFile();
        try {
            boolean res = file.createNewFile();
            return Mono.just(res);
        } catch (Exception e) {
            return Mono.just(file.exists());
        }
    }

    public Mono<String> copyFileTo(Path from, String to) {
        try {
            Path toPath = path(to);
            Files.createDirectories(toPath);
            Files.copy(from, path(to), StandardCopyOption.REPLACE_EXISTING);
            return Mono.just(to);
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    @Override
    public Mono<Boolean> deleteLock(String name) {
        return deleteObject(Bucket.LOCKS, name);
    }

    @Override
    public Mono<Boolean> deleteObject(Bucket bucket, String name) {
        try {
            Files.delete(path(name));
            return Mono.just(true);
        } catch (Exception e) {
            return Mono.just(false);
        }
    }

    private Path path(String name) {
        return Path.of(applicationConfig.getPmFilesPath(), name);
    }

}
