# Copyright 2010 Sean Cribbs, Sonian Inc., and Basho Technologies, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
require 'ripple'

module Ripple

  # Raised by <tt>save!</tt> when the document is invalid.  Use the
  # +document+ method to retrieve the document which did not validate.
  #   begin
  #     invalid_document.save!
  #   rescue Ripple::DocumentInvalid => invalid
  #     puts invalid.document.errors
  #   end
  class DocumentInvalid < StandardError
    include Translation
    attr_reader :document
    def initialize(document)
      @document = document
      errors = @document.errors.full_messages.join(", ")
      super(t("document_invalid", :errors => errors))
    end
  end

  module Validations
    extend ActiveSupport::Concern
    extend ActiveSupport::Autoload
    include ActiveModel::Validations

    autoload :AssociatedValidator

    module ClassMethods
      # @private
      def property(key, type, options={})
        prop = super
        validates key, prop.validation_options unless prop.validation_options.blank?
      end

      # Instantiates a new document, applies attributes from a block, and saves it
      # Raises Ripple::DocumentInvalid if the record did not save
      def create!(attrs={}, &block)
        obj = create(attrs, &block)
        (raise Ripple::DocumentInvalid.new(obj) if obj.new?) || obj
      end
    end

    module InstanceMethods
      # @private
      def save(options={:validate => true})
        return false if options[:validate] && !valid?
        super()
      end
      
      def save!
        (raise Ripple::DocumentInvalid.new(self) unless save) || true
      end
    end
  end
end

