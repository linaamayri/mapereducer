package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("district_containing_trees", DistrictContainingTrees.class,
                    "A map/reduce program that lists all the roundings having trees in Paris.");
            programDriver.addClass("species", Species.class,
                    "A map/reduce program that lists all the tree species in Paris.");
            programDriver.addClass("nbr_trees_species", NbrTreesSpecies.class,
                    "A map/reduce program that lists the number of trees by species in Paris.");
            programDriver.addClass("max_height_trees", MaxHeightTrees.class,
                    "A map/reduce program that give the tallest tree for each species.");
            programDriver.addClass("smallest_to_largest", SmallestToLargest.class,
                    "A map/reduce program that give the smallest to largest tree for all species.");


            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}