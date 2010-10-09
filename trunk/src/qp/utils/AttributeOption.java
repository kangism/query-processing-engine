package qp.utils;

public class AttributeOption {
    final Attribute attribute;
    final OrderByOption option;

    /**
     * ASC by default.
     * 
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

    /* (non-Javadoc)
     * @see java.lang.Object#clone()
     */
    public AttributeOption clone() {
	return new AttributeOption(attribute, option);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((attribute == null) ? 0 : attribute.hashCode());
	result = prime * result + ((option == null) ? 0 : option.hashCode());
	return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	AttributeOption other = (AttributeOption) obj;
	if (attribute == null) {
	    if (other.attribute != null)
		return false;
	} else if (!attribute.equals(other.attribute))
	    return false;
	if (option == null) {
	    if (other.option != null)
		return false;
	} else if (!option.equals(other.option))
	    return false;
	return true;
    }
    
    
}
