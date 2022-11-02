
SiteKeyBuild <- function(mlra, stateset, state) {
  
  # Build the base web services URL needed to retrieve the ecological site list
  classlist <- jsonlite::fromJSON(paste0("https://edit.jornada.nmsu.edu/services/downloads/esd/", mlra, "/class-list.json"))
  
  # Specify a state within MLRA by last two of site code, if stateset = TRUE
  if(stateset == TRUE) {
    # Read the ecological site list into a data table, skipping the the first two lines, which contain metadata
    ecoclasses <- as.data.frame(classlist$ecoclasses)
    ecoclasses <- dplyr::filter(ecoclasses, grepl(paste0(state), id)) # Subset within MLRA by state code
  } else {
    if(stateset == FALSE) {
      # Read the ecological site list into a data table, skipping the the first two lines, which contain metadata
      ecoclasses <- as.data.frame(classlist$ecoclasses)}
  }
  
  # Loop through ecological site list for MLRA and state to pull data of interest
  base.url <- paste0("https://edit.jornada.nmsu.edu/services/descriptions/esd/", mlra)
  
  # Make a list of all the data frames for the ecoclasses IDs
  doc.url.list <- lapply(X = ecoclasses$id, FUN = function(X) {
    jsonlite::fromJSON(paste0(base.url, "/", X, ".json"))
  })
  # Name all the indices of that list for easy retrieval
  names(doc.url.list) <- ecoclasses$id
  

  # Extract nominal key features
  # Landforms
  landforms <- data.frame(NULL)
    for(ecoclass.id in ecoclasses$id) {
      doc.url <- doc.url.list[[ecoclass.id]]
      landformlist <- data.frame(doc.url$physiographicFeatures$landforms$landform)
      landformlist <- as.data.frame(t(landformlist))
      if(ncol(landformlist) == 0) {
        next
      } else {
      landformlist <- tidyr::unite(landformlist, String, 1:ncol(landformlist), sep = " | ")
      landformlist$Property <- "Landform"
      landformlist$siteid <- ecoclass.id
      rownames(landformlist) <- NULL
      landforms <- rbind(landforms, landformlist)}}
  
  
  # Surface textures 
  surfacetextures <- data.frame(NULL)
  for(ecoclass.id in ecoclasses$id) {
    doc.url <- doc.url.list[[ecoclass.id]]
    surfacetextlist <- data.frame(doc.url$soilFeatures$texture)
    if(ncol(surfacetextlist) == 0) {
      next
    } else {
      modifiers <- dplyr::select(surfacetextlist, starts_with("mod"))
      modifiers <- data.frame(lapply(modifiers, as.character), stringsAsFactors = FALSE)
      modifiers <- modifiers %>%
        dplyr::mutate_all(na_if, "") %>%
        dplyr::mutate_all(.funs = tolower) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "gravelly", "GR"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "cobbly", "CB"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "stony", "ST"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "bouldery", "BY"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "channery", "CN"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "flaggy", "FL"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "cobbly", "CB"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "very", "V"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "extremely", "X"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "fine", ""))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "medium", ""))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., " ", ""))) 
      textures <- dplyr::select(surfacetextlist, starts_with("text"))
      textures <- data.frame(lapply(textures, as.character), stringsAsFactors = FALSE)
      textures <- textures %>%
        dplyr::mutate_all(na_if, "") %>%
        dplyr::mutate_all(.funs = tolower) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "coarse sand", "COS"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "very fine sandy loam", "VFSL"))) %>% 
        dplyr::mutate_all(funs(stringr::str_replace(., "fine sandy loam", "FSL"))) %>% 
        dplyr::mutate_all(funs(stringr::str_replace(., "sandy loam", "SL"))) %>% 
        dplyr::mutate_all(funs(stringr::str_replace(., "fine sand", "FS"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "very fine sand", "VFS"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "loamy coarse sand", "LCOS"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "loamy sand", "LS"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "loamy fine sand", "LFS"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "loamy very fine sand", "LVFS"))) %>%                                                                                        
        dplyr::mutate_all(funs(stringr::str_replace(., "coarse sandy loam", "COSL"))) %>% 
        dplyr::mutate_all(funs(stringr::str_replace(., "loamy", "L"))) %>% 
        dplyr::mutate_all(funs(stringr::str_replace(., "silt loam", "SIL"))) %>% 
        dplyr::mutate_all(funs(stringr::str_replace(., "sandy clay loam", "SCL"))) %>% 
        dplyr::mutate_all(funs(stringr::str_replace(., "silty clay loam", "SICL"))) %>% 
        dplyr::mutate_all(funs(stringr::str_replace(., "clay loam", "CL"))) %>% 
        dplyr::mutate_all(funs(stringr::str_replace(., "sandy clay", "SC"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "sandy clay", "SC"))) %>% 
        dplyr::mutate_all(funs(stringr::str_replace(., "silty clay", "SIC"))) %>% 
        dplyr::mutate_all(funs(stringr::str_replace(., "clay", "C"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "sand", "S"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "loam", "L"))) %>%
        dplyr::mutate_all(funs(stringr::str_replace(., "silt", "SI"))) %>% 
        dplyr::mutate_all(funs(stringr::str_replace(., " ", "")))
      surfacetextlist <- cbind(textures, modifiers)
      surfacetextlist <- dplyr::mutate(surfacetextlist, comb1 = ifelse(!is.na(modifier1), paste(modifier1, texture), texture))
      surfacetextlist <- dplyr::mutate(surfacetextlist, comb2 = ifelse(!is.na(modifier2), paste(modifier2, texture), texture))
      surfacetextlist <- dplyr::mutate(surfacetextlist, comb3 = ifelse(!is.na(modifier3), paste(modifier3, texture), texture))
      surfacetextlist <- dplyr::select(surfacetextlist, comb1:comb3)
      surfacetextlist <- tidyr::gather(surfacetextlist, variable, value, comb1:comb3)
      surfacetextlist <- surfacetextlist[, c(2)]
      surfacetextlist <- unique(surfacetextlist)
      surfacetextlist <- as.data.frame(t(surfacetextlist))
      surfacetextlist <- tidyr::unite(surfacetextlist, String, 1:ncol(surfacetextlist), sep = ",")
      surfacetextlist$Property <- "SurfaceTextures"
      surfacetextlist$siteid <- ecoclass.id
      surfacetextures <- rbind(surfacetextures, surfacetextlist)}
  }
  
  
  
  # Family particle size class
  # Currently if a site doesn't have a PSC listed, that site is missing from final df
  psc <- data.frame(NULL)
  for(ecoclass.id in ecoclasses$id) {
    doc.url <- doc.url.list[[ecoclass.id]]
    psclist <- data.frame(doc.url$soilFeatures$nominalProperties$value)
    if(nrow(psclist) == 0) {
      next
    } else {
      psclist$siteid <- ecoclass.id
      psclist$Property <- "PSC"
      psclist <- dplyr::select(psclist, String = 1, Property = 3, siteid = 2)
      psc <- rbind(psc, psclist) }
  }
  
  
  # Species list
  spcomp <- read.table(paste0("https://edit.jornada.nmsu.edu/services/downloads/esd/", mlra, "/rangeland-plant-composition.txt"), quote = "\"", fill = TRUE, sep="\t", skip = 2, header = TRUE)
  spcomp <- subset(spcomp, spcomp$Ecological.site.ID %in% ecoclasses$id)
  spcomp <- dplyr::select(spcomp, siteid = Ecological.site.ID, Ecosystem.state, 
                          Plant.symbol)
  spcomp <- spcomp %>%
    dplyr::filter(Ecosystem.state == 1) %>%
    dplyr::select(-Ecosystem.state) %>%
    dplyr::group_by(siteid) %>%
    dplyr::mutate(ID = row_number()) %>%
    dplyr::ungroup()
  
  spcomp <- tidyr::spread(spcomp, ID, Plant.symbol, fill = NA)
  spcomp <- tidyr::unite(spcomp, String, 2:ncol(spcomp), sep = ",", remove = TRUE, na.rm = TRUE)
  spcomp$Property <- "Species"
  spcomp <- dplyr::select(spcomp, String, Property, siteid)
  
  
  
  
  # Rbind into table
  nominaltable <- rbind(landforms, surfacetextures, psc, spcomp)
  # Reorder table for readability
  nominaltable <- dplyr::select(nominaltable, siteid, Property, String)
  # Sort table by siteid
  nominaltable <- dplyr::arrange(nominaltable, siteid, Property)
  

  
  
  
  # Build quantitative key 
  # Quantitative key features
  # Slope/elevation
  physio <- data.frame(NULL)
  for(ecoclass.id in ecoclasses$id) {
    doc.url <- doc.url.list[[ecoclass.id]]
    physiolist <- data.frame(doc.url$physiographicFeatures$intervalProperties)
    if(nrow(physiolist) == 0) {
      next
    } else {
      physiolist <- dplyr::filter(physiolist, property == "Elevation" | property == "Slope")
      physiolist$Low <- "Low"
      physiolist$High <- "High"
      physiolist <- tidyr::unite(physiolist, PropertyLow, property, Low, sep = "", remove = FALSE)
      physiolist <- tidyr::unite(physiolist, PropertyHigh, property, High, sep = "", remove = FALSE)
      physiolist <- physiolist[, c(1,5,2,6)]
      physiolist$siteid <- ecoclass.id
      physio <- rbind(physio, physiolist)
      physio$representativeLow <- as.numeric(physio$representativeLow)
      physio$representativeHigh <- as.numeric(physio$representativeHigh)}
  }
  
  
  # Flooding frequency
  flooding <- data.frame(NULL)
  for(ecoclass.id in ecoclasses$id) {
    doc.url <- doc.url.list[[ecoclass.id]]
    fflist <- as.data.frame(doc.url$physiographicFeatures$ordinalProperties)
    if(nrow(fflist) == 0) {
      next
    } else {
      fflist <- dplyr::filter(fflist, property == "Flooding frequency")
      fflist$Low <- "Low"
      fflist$High <- "High"
      fflist$property <- stringr::str_to_title(fflist$property)
      fflist$property <- gsub(" ", "", fflist$property)
      fflist <- tidyr::unite(fflist, PropertyLow, property, Low, sep = "", remove = FALSE)
      fflist <- tidyr::unite(fflist, PropertyHigh, property, High, sep = "", remove = FALSE)
      fflist <- fflist[, c(1,4,2,5)]
      fflist$siteid <- ecoclass.id
      flooding <- rbind(flooding, fflist)}
  }
  


  
  
  # Extracting avg precip low and high from climate narrative
  # Not sure how this would work in an MLRA where this info might be missing
  climate <- data.frame(NULL)
  for(ecoclass.id in ecoclasses$id) {
    doc.url <- doc.url.list[[ecoclass.id]] 
    precip <- unlist(doc.url$climaticFeatures$narratives$climaticFeatures)
    s <- unlist(stringi::stri_split(precip, regex = "(?<=[.])\\s"))
    q <- stringi::stri_detect(s, fixed = "annual precipitation")
    map <- s[q]
    nums <- data.frame(unlist(stringr::str_match_all(map, "[0-9]+")))
    if(nrow(nums) == 0) {
      next
    } else {
      if(nrow(nums) > 2) {
        next
      } else {
        lh <- c("representativeLow", "representativeHigh") #}
        nums$range <- lh
        nums$siteid <- ecoclass.id
        nums <- dplyr::rename(nums, AvgPrecipIn = 1)
        nums <- tidyr::spread(nums, range, AvgPrecipIn)
        nums$PropertyLow <- "AvgPrecipLow"
        nums$PropertyHigh <- "AvgPrecipHigh"
        nums <- nums[, c(4, 3, 5, 2, 1)]
        climate <- rbind(climate, nums)
        climate$representativeLow <- as.numeric(climate$representativeLow)
        climate$representativeHigh <- as.numeric(climate$representativeHigh)}
    }}
    
    # Soil depth
    soildepth <- data.frame(NULL)
    for(ecoclass.id in ecoclasses$id) {
      doc.url <- doc.url.list[[ecoclass.id]] 
      depthlist <- data.frame(doc.url$soilFeatures$intervalProperties)
      if(nrow(depthlist) == 0) {
        next
      } else {
        depthlist <- dplyr::filter(depthlist, property == "Soil depth")
        if(nrow(depthlist) == 0) {
          next
        } else {
          depthlist <- dplyr::mutate_all(depthlist, na_if, "")
          depthlist$PropertyLow <- "SoilDepthLow"
          depthlist$PropertyHigh <- "SoilDepthHigh"
          depthlist <- depthlist[, c(7,3,8,4)]
          depthlist$siteid <- ecoclass.id
          soildepth <- rbind(soildepth, depthlist)
          soildepth$representativeLow <- as.numeric(soildepth$representativeLow)
          soildepth$representativeHigh <- as.numeric(soildepth$representativeHigh)}}
    }
    
    # Surface Fragments
    surffrags <- data.frame(NULL)
    for(ecoclass.id in ecoclasses$id) {
      doc.url <- doc.url.list[[ecoclass.id]]
      surffraglist <- data.frame(doc.url$soilFeatures$intervalProperties)
      if(nrow(surffraglist) == 0) {
        next
      } else {
        surffraglist <- dplyr::filter(surffraglist, grepl("fragment", property))
        surfgr <- dplyr::filter(surffraglist, grepl("<", property))
        if(nrow(surfgr) == 0) {
          next
        } else {
          surfgr$PropertyLow <- "SurfaceGravelLow"
          surfgr$PropertyHigh <- "SurfaceGravelHigh"
          surfgr <- surfgr[, c(7, 3, 8, 4)]
          surflg <- dplyr::filter(surffraglist, grepl(">", property))
          if(nrow(surflg) == 0) {
            next
          } else {
            surflg$PropertyLow <- "SurfaceLGFragsLow"
            surflg$PropertyHigh <- "SurfaceLGFragsHigh"
            surflg <- surflg[, c(7, 3, 8, 4)]
            surffraglist <- rbind(surfgr, surflg)
            surffraglist$siteid <- ecoclass.id
            surffrags <- rbind(surffrags, surffraglist)
            surffrags$representativeLow <- as.numeric(surffrags$representativeLow)
            surffrags$representativeHigh <- as.numeric(surffrags$representativeHigh)}
        }}}
    
    
    # Subsurface fragments
    subsurffrags <- data.frame(NULL)
    for(ecoclass.id in ecoclasses$id) {
      doc.url <- doc.url.list[[ecoclass.id]]
      ssfraglist <- data.frame(doc.url$soilFeatures$profileProperties)
      if(nrow(ssfraglist) == 0) {
        next
      } else {
        ssfraglist <- dplyr::filter(ssfraglist, grepl("fragment", property))
        ssgr <- dplyr::filter(ssfraglist, grepl("<", property))
        if(nrow(ssgr) == 0) {
          next
        } else {
          ssgr$PropertyLow <- "SubsurfGravelLow"
          ssgr$PropertyHigh <- "SubsurfGravelHigh"
          ssgr <- ssgr[, c(9, 5, 10, 6)]
          sslg <- dplyr::filter(ssfraglist, grepl(">", property))
          if(nrow(sslg) == 0) {
            next
          } else {
            sslg$PropertyLow <- "SubsurfLGFragsLow"
            sslg$PropertyHigh <- "SubsurfLGFragsHigh"
            sslg <- sslg[, c(9, 5, 10, 6)]
            ssfraglist <- rbind(ssgr, sslg)
            ssfraglist$siteid <- ecoclass.id
            subsurffrags <- rbind(subsurffrags, ssfraglist)
            subsurffrags$representativeLow <- as.numeric(subsurffrags$representativeLow)
            subsurffrags$representativeHigh <- as.numeric(subsurffrags$representativeHigh)}
        }}}
    
    
    
    # Run-in 
    drainclass <- data.frame(NULL)
    for(ecoclass.id in ecoclasses$id) {
      doc.url <- doc.url.list[[ecoclass.id]]
      dclist <- data.frame(doc.url$soilFeatures$ordinalProperties)
      if(nrow(dclist) == 0) {
        next
      } else {
        dclist <- dclist[c(1), c(2, 3)]
        dclist$siteid <- ecoclass.id
        dclist$PropertyLow <- "DrainageClassLow"
        dclist$PropertyHigh <- "DrainageClassHigh"
        dclist <- dplyr::select(dclist, PropertyLow, representativeLow, PropertyHigh,
                                representativeHigh, siteid)
        drainclass <- rbind(drainclass, dclist) }
    }
    
    # Permeability
    permclass <- data.frame(NULL)
    for(ecoclass.id in ecoclasses$id) {
      doc.url <- doc.url.list[[ecoclass.id]]
      permlist <- data.frame(doc.url$soilFeatures$ordinalProperties)
      if(nrow(permlist) == 0) {
        next
      } else {
        permlist <- permlist[c(2), c(2, 3)]
        permlist$siteid <- ecoclass.id
        permlist$PropertyLow <- "PermeablityClassLow"
        permlist$PropertyHigh <- "PermeabilityClassHigh"
        permlist <- dplyr::select(permlist, PropertyLow, representativeLow, PropertyHigh,
                                representativeHigh, siteid)
        permclass <- rbind(permclass, permlist) }
    }
    
    
    # Electrical conductivity
    eleccond <- data.frame(NULL)
    for(ecoclass.id in ecoclasses$id) {
      doc.url <- doc.url.list[[ecoclass.id]]
      eclist <- data.frame(doc.url$soilFeatures$profileProperties)
      if(nrow(eclist) == 0) {
        next
      } else {
        eclist <- dplyr::filter(eclist, grepl("conductivity", property))
        if(nrow(eclist) == 0) {
          next
        } else {
        eclist <- eclist[, c(5, 6)]
        eclist$siteid <- ecoclass.id
        eclist$PropertyLow <- "ElectricalConductivity(mmhos/cmO)Low"
        eclist$PropertyHigh <- "ElectricalConductivity(mmhos/cmO)High"
        eclist <- dplyr::select(eclist, PropertyLow, representativeLow, PropertyHigh,
                                  representativeHigh, siteid)
        eleccond <- rbind(eleccond, eclist) }}
    }
    
    # Soil pH
    ph <- data.frame(NULL)
    for(ecoclass.id in ecoclasses$id) {
      doc.url <- doc.url.list[[ecoclass.id]]
      phlist <- data.frame(doc.url$soilFeatures$profileProperties)
      if(nrow(phlist) == 0) {
        next
      } else {
        phlist <- dplyr::filter(phlist, grepl("reaction", property))
        if(nrow(phlist) == 0) {
          next
        } else {
        phlist <- phlist[, c(5, 6)]
        phlist$siteid <- ecoclass.id
        phlist$PropertyLow <- "SoilpHLow"
        phlist$PropertyHigh <- "SoilpHHigh"
        phlist <- dplyr::select(phlist, PropertyLow, representativeLow, PropertyHigh,
                                representativeHigh, siteid)
        ph <- rbind(ph, phlist) }}
    }
    
    # CC 
    cc <- data.frame(NULL)
    for(ecoclass.id in ecoclasses$id) {
      doc.url <- doc.url.list[[ecoclass.id]]
      cclist <- data.frame(doc.url$soilFeatures$profileProperties)
      if(nrow(cclist) == 0) {
        next
      } else {
      cclist <- dplyr::filter(cclist, grepl("carbonate", property))
        if(nrow(cclist) == 0) {
          next
        } else {
          cclist <- cclist[, c(5, 6)]
          cclist$siteid <- ecoclass.id
          cclist$PropertyLow <- "CalciumCarbonateEquivalentLow"
          cclist$PropertyHigh <- "CalciumCarbonateEquivalentHigh"
          cclist <- dplyr::select(cclist, PropertyLow, representativeLow, PropertyHigh,
                                  representativeHigh, siteid)
          cc <- rbind(cc, cclist) }}
    }
    
    
    
    # AWC
    awc <- data.frame(NULL)
    for(ecoclass.id in ecoclasses$id) {
      doc.url <- doc.url.list[[ecoclass.id]]
      awclist <- data.frame(doc.url$soilFeatures$profileProperties)
      if(nrow(awclist) == 0) {
        next
      } else {
        awclist <- dplyr::filter(awclist, grepl("Available water", property))
        if(nrow(awclist) == 0) {
          next
        } else {
          awclist <- awclist[, c(5, 6)]
          awclist$siteid <- ecoclass.id
          awclist$PropertyLow <- "AWCLow"
          awclist$PropertyHigh <- "AWCHigh"
          awclist <- dplyr::select(awclist, PropertyLow, representativeLow, PropertyHigh,
                                  representativeHigh, siteid)
          awc <- rbind(awc, awclist) }}
    }
    
    # SAR
    sar <- data.frame(NULL)
    for(ecoclass.id in ecoclasses$id) {
      doc.url <- doc.url.list[[ecoclass.id]]
      sarlist <- data.frame(doc.url$soilFeatures$profileProperties)
      if(nrow(sarlist) == 0) {
        next
      } else {
        sarlist <- dplyr::filter(sarlist, grepl("adsorption", property))
        if(nrow(sarlist) == 0) {
          next
        } else {
          sarlist <- sarlist[, c(5, 6)]
          sarlist$siteid <- ecoclass.id
          sarlist$PropertyLow <- "SodiumAdsorptionRatioLow"
          sarlist$PropertyHigh <- "SodiumAdsorptionRatioHigh"
          sarlist <- dplyr::select(sarlist, PropertyLow, representativeLow, PropertyHigh,
                                   representativeHigh, siteid)
          sar <- rbind(sar, sarlist) }}
    }
    
    
    
    
    
    
    
    
    # Join quantitative site properties into key table
    quantkeytable <- rbind(physio, climate, soildepth, surffrags, subsurffrags, awc,
                           cc, drainclass, eleccond, permclass, ph, sar, flooding)
    # Add relational columns
    quantkeytable$Lower.Relation <- ">="
    quantkeytable$Upper.Relation <- "<="
    # Add a property column that can be used to join plot data to key
    quantkeytable$Property <- stringr::str_sub(quantkeytable$PropertyLow, start = 1, end = -4 )
    # Create artificial max for soil depth
    quantkeytable <- dplyr::mutate(quantkeytable, representativeHigh = ifelse(Property == "SoilDepth" & is.na(representativeHigh), 100, representativeHigh))
    # Reorder table for readability
    quantkeytable <- dplyr::select(quantkeytable, siteid, PropertyLow, Lower.Relation,
                                   representativeLow, PropertyHigh, Upper.Relation,
                                   representativeHigh, Property)
    
    # Format nominal table to join 
    nominaltable$PropertyHigh <- nominaltable$Property
    nominaltable$PropertyLow <- nominaltable$Property
    nominaltable$representativeHigh <- NA
    nominaltable$Lower.Relation <- "=="
    nominaltable$Upper.Relation <- NA
    nominaltable <- dplyr::select(nominaltable, siteid, PropertyLow, Lower.Relation,
                                  representativeLow = String, PropertyHigh, 
                                  Upper.Relation, representativeHigh, Property)
    
    
    # Join nominal and quantitative tables
    keytable <- rbind(quantkeytable, nominaltable)
    
    # Sort by ecological site
    keytable <- dplyr::arrange(keytable, siteid, Property)
  
  
  return(keytable)
  
} 

