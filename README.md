Jinx Migrator
=============

**Home**:         [http://github.com/jinx/migrate](http://github.com/jinx/migrate)    
**Git**:          [http://github.com/jinx/migrate](http://github.com/jinx/migrate)       
**Author**:       OHSU Knight Cancer Institute    
**Copyright**:    2012    
**License**:      MIT License    

Synopsis
--------
Jinx JSON enables JSON serialization for [Jinx](http://github.com/jinx/core) resources.

Installing
----------
Jinx JSON is installed as a JRuby gem:

    [sudo] jgem install jinx-migrate

Usage
-----
Enable Jinx for a Java package, as described in the [Jinx](http://github.com/jinx/core) Usage.

    require 'jinx/migrate'
    migrateed = jinxed.to_migrate #=> the JSON representation of a jinxed resource
    JSON[migrateed] #=> the deserialized jinxed resource

Copyright
---------
Jinx &copy; 2012 by [Oregon Health & Science University](http://www.ohsu.edu/xd/health/services/cancer/index.cfm).
Jinx is licensed under the MIT license. Please see the LICENSE and LEGAL files for more information.
