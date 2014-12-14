/*-
 * See the file LICENSE for redistributiion information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationTypeDoc;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.ParameterizedType;
import com.sun.javadoc.Type;
import com.sun.javadoc.TypeVariable;
import com.sun.javadoc.WildcardType;

class HidingParameterizedTypeWrapper extends HidingWrapper 
                                     implements ParameterizedType {
    public HidingParameterizedTypeWrapper(ParameterizedType type, 
                                          Map mapWrappers) {
        super(type, mapWrappers);
    }

    private ParameterizedType _getParameterizedType() {
        return (ParameterizedType)getWrappedObject();
    }

    public ClassDoc asClassDoc() {
        return (ClassDoc) wrapOrHide(_getParameterizedType().asClassDoc());
    }

    public Type containingType() {
        return (Type)wrapOrHide(_getParameterizedType().containingType());
    }

    public Type[] interfaceTypes() {
        return (Type[])wrapOrHide(_getParameterizedType().interfaceTypes());
    }

    public Type superclassType() {
        return (Type)wrapOrHide(_getParameterizedType().superclassType());
    }

    public Type[] typeArguments() {
        return (Type[])wrapOrHide(_getParameterizedType().typeArguments());
    }
    
    public String dimension() {
        return _getParameterizedType().dimension();
    }

    public String qualifiedTypeName() {
        return _getParameterizedType().qualifiedTypeName();
    }

    public String toString() {
        return _getParameterizedType().toString();
    }

    public String typeName() {
        return _getParameterizedType().typeName();
    }

    public AnnotationTypeDoc asAnnotationTypeDoc() {
        return (AnnotationTypeDoc)
                wrapOrHide(_getParameterizedType().asAnnotationTypeDoc());
    }

    public WildcardType asWildcardType() {
        return (WildcardType)
                wrapOrHide(_getParameterizedType().asWildcardType());
    }

    public TypeVariable asTypeVariable() {
        return (TypeVariable)
                wrapOrHide(_getParameterizedType().asTypeVariable());
    }

    public ParameterizedType asParameterizedType() {
        return (ParameterizedType)
                wrapOrHide(_getParameterizedType().asParameterizedType());
    }

    public boolean isPrimitive() {
        return _getParameterizedType().isPrimitive();
    }

    public String simpleTypeName() {
        return _getParameterizedType().simpleTypeName();
    }
}
