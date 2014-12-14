/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.AnnotationTypeDoc;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.ConstructorDoc;
import com.sun.javadoc.FieldDoc;
import com.sun.javadoc.MethodDoc;
import com.sun.javadoc.PackageDoc;
import com.sun.javadoc.ParamTag;
import com.sun.javadoc.ParameterizedType;
import com.sun.javadoc.SeeTag;
import com.sun.javadoc.SourcePosition;
import com.sun.javadoc.Tag;
import com.sun.javadoc.Type;
import com.sun.javadoc.TypeVariable;
import com.sun.javadoc.WildcardType;

class HidingClassDocWrapper extends HidingWrapper implements ClassDoc {
    
    public HidingClassDocWrapper(ClassDoc classdoc, Map mapWrappers) {
        super(classdoc, mapWrappers);
    }

    private ClassDoc _getClassDoc() {
        return (ClassDoc)getWrappedObject();
    }

    public ConstructorDoc[] constructors() {
        return (ConstructorDoc[])wrapOrHide(_getClassDoc().constructors());
    }

    public ConstructorDoc[] constructors(boolean filter) {
        return (ConstructorDoc[])
                wrapOrHide(_getClassDoc().constructors(filter));
    }

    public boolean definesSerializableFields() {
        return _getClassDoc().definesSerializableFields();
    }

    public FieldDoc[] fields() {
        return (FieldDoc[])wrapOrHide(_getClassDoc().fields());
    }

    public FieldDoc[] fields(boolean filter) {
        return (FieldDoc[])wrapOrHide(_getClassDoc().fields(filter));
    }

    public ClassDoc findClass(String szClassName) {
        return (ClassDoc)wrapOrHide(_getClassDoc().findClass(szClassName));
    }

    /**
     * @deprecated as of 11.0
     */
    public ClassDoc[] importedClasses() {
        return (ClassDoc[])wrapOrHide(_getClassDoc().importedClasses());
    }

    /**
     * @deprecated as of 11.0
     */
    public PackageDoc[] importedPackages() {
        return (PackageDoc[])wrapOrHide(_getClassDoc().importedPackages());
    }

    public ClassDoc[] innerClasses() {
        return (ClassDoc[])wrapOrHide(_getClassDoc().innerClasses());
    }

    public ClassDoc[] innerClasses(boolean filter) {
        return (ClassDoc[])wrapOrHide(_getClassDoc().innerClasses(filter));
    }

    public ClassDoc[] interfaces() {
        return (ClassDoc[])wrapOrHide(_getClassDoc().interfaces());
    }

    public boolean isAbstract() {
        return _getClassDoc().isAbstract();
    }

    public boolean isExternalizable() {
        return _getClassDoc().isExternalizable();
    }

    public boolean isSerializable() {
        return _getClassDoc().isSerializable();
    }

    public MethodDoc[] methods() {
        return (MethodDoc[])wrapOrHide(_getClassDoc().methods());
    }

    public MethodDoc[] methods(boolean filter) {
        return (MethodDoc[])wrapOrHide(_getClassDoc().methods(filter));    
    }

    public FieldDoc[] serializableFields() {
        return (FieldDoc[])wrapOrHide(_getClassDoc().serializableFields());
    }

    public MethodDoc[] serializationMethods() {
        return (MethodDoc[])wrapOrHide(_getClassDoc().serializationMethods());
    }

    public boolean subclassOf(ClassDoc classdoc) {
        if (classdoc instanceof HidingClassDocWrapper) {
            classdoc = (ClassDoc)
                       ((HidingClassDocWrapper)classdoc).getWrappedObject();
        }
    
        return _getClassDoc().subclassOf(classdoc);
    }

    public ClassDoc superclass() {
        return (ClassDoc)wrapOrHide(_getClassDoc().superclass());
    }

    public ClassDoc asClassDoc() {
        return this;
    }

    public String dimension() {
        return _getClassDoc().dimension();
    }

    public String qualifiedTypeName() {
        return _getClassDoc().qualifiedTypeName();
    }

    public String toString() {
        return _getClassDoc().toString();
    }

    public String typeName() {
        return _getClassDoc().typeName();
    }

    public FieldDoc[] enumConstants() {
        return (FieldDoc[])wrapOrHide(_getClassDoc().enumConstants());
    }

    public ParamTag[] typeParamTags() {
        return (ParamTag[]) wrapOrHide(_getClassDoc().typeParamTags());
    }

    public TypeVariable[] typeParameters() {
        return (TypeVariable[]) wrapOrHide(_getClassDoc().typeParameters());
    }

    public Type[] interfaceTypes() {
        return (Type[]) wrapOrHide(_getClassDoc().interfaceTypes());
    }

    public Type superclassType() {
        return (Type) wrapOrHide(_getClassDoc().superclassType());
    }

    public AnnotationTypeDoc asAnnotationTypeDoc() {
        return (AnnotationTypeDoc)
                wrapOrHide(_getClassDoc().asAnnotationTypeDoc());
    }

    public WildcardType asWildcardType() {
        return (WildcardType)wrapOrHide(_getClassDoc().asWildcardType());
    }

    public TypeVariable asTypeVariable() {
        return (TypeVariable)wrapOrHide(_getClassDoc().asTypeVariable());
    }

    public ParameterizedType asParameterizedType() {
        return (ParameterizedType)
                wrapOrHide(_getClassDoc().asParameterizedType());
    }

    public boolean isPrimitive() {
        return _getClassDoc().isPrimitive();
    }

    public String simpleTypeName() {
        return _getClassDoc().simpleTypeName();
    }
  
    public String commentText() {
        return _getClassDoc().commentText();
    }

    public int compareTo(Object obj) {
        if (obj instanceof HidingWrapper) {
            return _getClassDoc().
                   compareTo(((HidingWrapper)obj).getWrappedObject());
        } else {
            return _getClassDoc().compareTo(obj);
        }
    }

    public Tag[] firstSentenceTags() {
        return (Tag[])wrapOrHide(_getClassDoc().firstSentenceTags());
    }

    public String getRawCommentText() {
        return _getClassDoc().getRawCommentText();
    }

    public Tag[] inlineTags() {
        return (Tag[])wrapOrHide(_getClassDoc().inlineTags());
    }

    public boolean isClass() {
        return _getClassDoc().isClass();
    }

    public boolean isConstructor() {
        return _getClassDoc().isConstructor();
    }

    public boolean isError() {
        return _getClassDoc().isError();
    }

    public boolean isException() {
        return _getClassDoc().isException();
    }

    public boolean isField() {
        return _getClassDoc().isField();
    }

    public boolean isIncluded() {
        return _getClassDoc().isIncluded();
    }

    public boolean isInterface() {
        return _getClassDoc().isInterface();
    }

    public boolean isMethod() {
        return _getClassDoc().isMethod();
    }

    public boolean isOrdinaryClass() {
        return _getClassDoc().isOrdinaryClass();
    }

    public String name() {
        return _getClassDoc().name();
    }

    public SeeTag[] seeTags() {
        return (SeeTag[])wrapOrHide(_getClassDoc().seeTags());
    }

    public void setRawCommentText(String szText) {
        _getClassDoc().setRawCommentText(szText);
    }

    public Tag[] tags() {
        return (Tag[])wrapOrHide(_getClassDoc().tags());
    }

    public Tag[] tags(String szTagName) {
        return (Tag[])wrapOrHide(_getClassDoc().tags(szTagName));
    }
  
    public SourcePosition position() {
        return (SourcePosition) wrapOrHide(_getClassDoc().position());
    }

    public boolean isAnnotationType() {
        return _getClassDoc().isAnnotationType();
    }

    public boolean isEnum() {
        return _getClassDoc().isEnum();
    }

    public boolean isAnnotationTypeElement() {
        return _getClassDoc().isAnnotationTypeElement();
    }

    public boolean isEnumConstant() {
        return _getClassDoc().isEnumConstant();
    }
  
    public ClassDoc containingClass() {
        return (ClassDoc)wrapOrHide(_getClassDoc().containingClass());
    }
    
    public PackageDoc containingPackage() {
        return (PackageDoc)wrapOrHide(_getClassDoc().containingPackage());
    }

    public boolean isFinal() {
        return _getClassDoc().isFinal();
    }

    public boolean isPackagePrivate() {
        return _getClassDoc().isPackagePrivate();
    }

    public boolean isPrivate() {
        return _getClassDoc().isPrivate();
    }

    public boolean isProtected() {
        return _getClassDoc().isProtected();
    }

    public boolean isPublic() {
        return _getClassDoc().isPublic();
    }

    public boolean isStatic() {
        return _getClassDoc().isStatic();
    }

    public int modifierSpecifier() {
        return _getClassDoc().modifierSpecifier();
    }

    public String modifiers() {
        return _getClassDoc().modifiers();
    }

    public String qualifiedName() {
        return _getClassDoc().qualifiedName();
    }

    public AnnotationDesc[] annotations() {
        return (AnnotationDesc[])wrapOrHide(_getClassDoc().annotations());
    }
}
