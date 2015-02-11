package rss.categorizer.model;

/**
 * Created by minza on 2/1/15.
 */



public class Label {

//    enum Labels {
//        politics, business, health, technology, entertainment, sports
//    }

    String labelName;
    double numericLabel;

    public Label(String name) {
        this.labelName = name;

        switch (labelName) {
            case "politics":
                this.numericLabel = 0.0;
                break;
            case "business":
                this.numericLabel = 1.0;
                break;
            case "health":
                this.numericLabel = 2.0;
                break;
            case "technology":
                this.numericLabel = 3.0;
                break;
            case "entertainment":
                this.numericLabel = 4.0;
                break;
            case "sports":
                this.numericLabel = 5.0;
                break;
            default:
                this.numericLabel = -1.0;
                System.out.println("label does not fit");

        }

    }

        public double getNumericalValue(){
        return this.numericLabel;
    }

    public String getLabelName(){
        return this.labelName;
    }

}
