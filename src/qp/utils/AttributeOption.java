package qp.utils;

public class AttributeOption {
   final Attribute attribute;
   final OrderByOption option;
   
   
   /**
    * ASC by default.
    * @param attribute
    */
   public AttributeOption(Attribute attribute) {
       this.attribute = attribute;
       this.option = OrderByOption.ASC;
   }   
   
/**
 * @param attribute
 * @param option
 */
public AttributeOption(Attribute attribute, OrderByOption option) {
    this.attribute = attribute;
    this.option = option;
}

/**
 * @return the attribute
 */
public Attribute getAttribute() {
    return attribute;
}
/**
 * @return the option
 */
public OrderByOption getOption() {
    return option;
}
    
public AttributeOption clone(){
    return new AttributeOption(attribute,option);
}
}
