module Jinx
  # The Migratable mix-in adds migration support for Resource domain objects.
  # For each migration Resource created by a Migrator, the migration process
  # is as follows:
  #
  # 1. The migrator creates the Resource using the empty constructor.
  #
  # 2. Each input field value which maps to a Resource attribute is obtained from the
  #    migration source.
  #
  # 3. If the Resource class implements a method +migrate_+_attribute_ for the
  #    migration _attribute_, then that migrate method is called with the input value
  #    argument. If there is a migrate method, then the attribute is set to the
  #    result of calling that method, otherwise the attribute is set to the original
  #    input value.
  #
  #    For example, if the +Name+ input field maps to +Parent.name+, then a
  #    custom +Parent+ +migrate_name+ shim method can be defined to reformat
  #    the input name.
  #
  # 4. The Resource attribute is set to the (possibly modified) value.
  #
  # 5. After all input fields are processed, then {#migration_valid?} is called to
  #    determine whether the migrated object can be used. {#migration_valid?} is true
  #    by default, but a migration shim can add a validation check,
  #    migrated Resource class to return false for special cases.
  #
  #    For example, a custom +Parent+ +migration_valid?+ shim method can be
  #    defined to return whether there is a non-empty input field value.
  #
  # 6. After the migrated objects are validated, then the Migrator fills in
  #    dependency hierarchy gaps. For example, if the Resource class +Parent+
  #    owns the +household+ dependent which in turn owns the +address+ dependent
  #    and the migration has created a +Parent+ and an +Address+ but no +Household+,
  #    then an empty +Household+ is created which is owned by the migrated +Parent+
  #    and owns the migrated +Address+.
  #
  # 7. After all dependencies are filled in, then the independent references are set
  #    for each created Resource (including the new dependents). If a created
  #    Resource has an independent non-collection Resource reference attribute
  #    and there is a migrated instance of that attribute type, then the attribute
  #    is set to that migrated instance.
  #
  #    For example, if +Household+ has a +address+ attribute and there is a
  #    single migrated +Address+ instance, then the +address+ attribute is set
  #    to that migrated +Address+ instance.
  #
  #    If the referencing class implements a method +migrate_+_attribute_ for the
  #    migration _attribute_, then that migrate method is called with the referenced
  #    instance argument. The result is used to set the attribute. Otherwise, the
  #    attribute is set to the original referenced instance.
  #
  #    There must be a single unambiguous candidate independent instance, e.g. in the
  #    unlikely but conceivable case that two +Address+ instances are migrated, then the
  #    +address+ attribute is not set. Similarly, collection attributes are not set,
  #    e.g. a +Address+ +protocols+ attribute is not set to a migrated +Protocol+
  #    instance.
  #
  # 8. The {#migrate} method is called to complete the migration. As described in the
  #    method documentation, a migration shim Resource subclass can override the
  #    method for custom migration processing, e.g. to migrate the ambiguous or
  #    collection attributes mentioned above, or to fill in missing values.
  #
  #    Note that there is an extensive set of attribute defaults defined in the +Jinx::Resource+
  #    application domain classes. These defaults are applied in a migration database save
  #    action and need not be set in a migration shim. For example, if an acceptable
  #    default for an +Address.country+ property is defined in the +Address+ meta-data,
  #    then the country does not need to be set in a migration shim.
  module Migratable
    # Completes setting this Migratable domain object's attributes from the given input row.
    # This method is responsible for migrating attributes which are not mapped
    # in the configuration. It is called after the configuration attributes for
    # the given row are migrated and before {#migrate_references}.
    #
    # This base implementation is a no-op.
    # Subclasses can modify this method to complete the migration. The overridden
    # methods should call +super+ to pick up the superclass migration.
    #
    # @param [{Symbol => Object}] row the input row field => value hash
    # @param [<Resource>] migrated the migrated instances, including this domain object
    def migrate(row, migrated)
    end

    # Returns whether this migration target domain object is valid. The default is true.
    # A migration shim should override this method on the target if there are conditions
    # which determine whether the migration should be skipped for this target object.
    #
    # @return [Boolean] whether this migration target domain object is valid
    def migration_valid?
      true
    end

    # Migrates this domain object's migratable references. This method is called by the
    # Migrator and should not be overridden by subclasses. Subclasses tailor
    # individual reference attribute migration by defining a +migrate_+_attribute_ method
    # for the _attribute_ to modify.
    #
    # The migratable reference attributes consist of the non-collection saved independent
    # attributes and the unidirectional dependent attributes which don't already have a value.
    # For each such migratable attribute, if there is a single instance of the attribute
    # type in the given migrated domain objects, then the attribute is set to that
    # migrated instance.
    #
    # If the attribute is associated with a method in proc_hash, then that method is called
    # on the migrated instance and input row. The attribute is set to the method return value.
    # proc_hash includes an entry for each +migrate_+_attribute_ method defined by this
    # Resource's class.
    #
    # @param [{Symbol => Object}] row the input row field => value hash
    # @param [<Resource>] migrated the migrated instances, including this Resource
    # @param [Class] target the migration target class
    # @param [{Symbol => Proc}, nil] proc_hash a hash that associates this domain object's
    #   attributes to a migration shim block
    def migrate_references(row, migrated, target, proc_hash=nil)
      # migrate the owner
      migratable__migrate_owner(row, migrated, target, proc_hash)
      # migrate the remaining attributes
      migratable__set_nonowner_references(migratable_independent_attributes, row, migrated, proc_hash)
      migratable__set_nonowner_references(self.class.unidirectional_dependent_attributes, row, migrated, proc_hash)
    end
                 
    # Returns this Resource's class {Propertied#independent_attributes}.
    # Applications can override this implement to restrict the independent attributes which
    # are migrated, e.g. to include only saved independent attributes. 
    #
    # @return the attributes to migrate
    def migratable_independent_attributes
      self.class.independent_attributes
    end
    
    # Extracts the content of this migration target to the given file.
    #
    # This base implementation is a no-op.
    # Subclasses can modify this method to write data to the extract.
    #
    # @param [IO] file the extract output stream 
    def extract(file)
    end
    
    private
    
    # Migrates the owner as follows:
    # * If there is exactly one migrated owner, then the owner reference is
    #   set to that owner.
    # * Otherwise, if there is more than one owner but only one owner instance
    #   of the given target class, then that target instance is that owner.
    # * Otherwise, no reference is set.
    #
    # @param row (see #migrate_references)
    # @param migrated (see #migrate_references)
    # @param target (see #migrate_references)
    # @param proc_hash (see #migrate_references)
    # @return [Resource, nil] the migrated owner, if any
    def migratable__migrate_owner(row, migrated, target, proc_hash=nil)
      # the owner attributes=> migrated reference hash
      ovh = self.class.owner_attributes.to_compact_hash do |mattr|
        pa = self.class.property(mattr)
        migratable__target_value(pa, row, migrated, proc_hash)
      end
      # If there is more than one owner candidate, then select the owner
      # attribute which references the target. If there is more than one
      # such attribute, then select the preferred owner.
      if ovh.size > 1 then
        tvh = ovh.filter_on_value { |ov| target === ov }.to_hash
        if tvh.size == 1 then
          ovh = tvh
        else
          ownrs = ovh.values.uniq
          if ownrs.size == 1 then
            ovh = {ovh.keys.first => ownrs.first}
          else
            logger.debug { "The migrated dependent #{qp} has ambiguous migrated owner references #{ovh.qp}." }
            preferred = migratable__preferred_owner(ownrs)
            if preferred then
              logger.debug { "The preferred dependent #{qp} migrated owner reference is #{preferred.qp}." }
              ovh = {ovh.keys.detect { |k| ovh[k] == preferred } => preferred}
            end
          end
        end
      end
      if ovh.size == 1 then
        oattr, oref = ovh.first
        set_property_value(oattr, oref)
        logger.debug { "Set the #{qp} #{oattr} owner to the migrated #{oref.qp}." }
      end
      oref
    end
    
    # This base implementation returns nil. Subclasses can override this to select a preferred owner.
    #
    # @param [<Resource>] candidates the migrated owners
    # @return [Resource] the preferred owner
    def migratable__preferred_owner(candidates)
      nil
    end
    
    # @param [Property::Filter] the attributes to set
    # @param row (see #migrate_references)
    # @param migrated (see #migrate_references)
    # @param proc_hash (see #migrate_references)
    def migratable__set_nonowner_references(attr_filter, row, migrated, proc_hash=nil)
      attr_filter.each_pair do |mattr, pa|
        # skip owners
        next if pa.owner?
        # the target value
        ref = migratable__target_value(pa, row, migrated, proc_hash) || next
        if pa.collection? then
          # the current value
          value = send(pa.reader) || next
          value << ref
          logger.debug { "Added the migrated #{ref.qp} to #{qp} #{mattr}." }
        else 
          current = send(mattr)
          if current then
            logger.debug { "Ignoring the migrated #{ref.qp} since #{qp} #{mattr} is already set to #{current.qp}." }
          else
            set_property_value(mattr, ref)
            logger.debug { "Set the #{qp} #{mattr} to the migrated #{ref.qp}." }
          end
        end
      end
    end
    
    # @param [Property] pa the reference attribute
    # @param row (see #migrate_references)
    # @param migrated (see #migrate_references)
    # @param proc_hash (see #migrate_references)
    # @return [Resource, nil] the migrated instance of the given class, or nil if there is not
    #   exactly one such instance
    def migratable__target_value(pa, row, migrated, proc_hash=nil)
      # the migrated references which are instances of the attribute type
      refs = migrated.select { |other| other != self and pa.type === other }
      # skip ambiguous references
      if refs.size > 1 then logger.debug { "Migrator did not set references to ambiguous targets #{refs.pp_s}." } end
      return unless refs.size == 1
      # the single reference
      ref = refs.first
      # the shim method, if any
      proc = proc_hash[pa.to_sym] if proc_hash
      # if there is a shim method, then call it
      proc ? proc.call(self, ref, row) : ref
    end
  end
end