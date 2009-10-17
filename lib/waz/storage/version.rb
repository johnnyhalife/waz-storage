module WAZ
  module Storage
    module VERSION #:nodoc:
      MAJOR    = '0'
      MINOR    = '5'
      TINY     = '4' 
    end
    
    Version = [VERSION::MAJOR, VERSION::MINOR, VERSION::TINY].compact * '.'
  end
end